"""Microscope, Slide / prep and Notes cards for the Live Lab tab (split out of ``ui/live_lab_tab.py``)."""
from __future__ import annotations

import json
from datetime import datetime

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QListWidget,
    QFormLayout,
    QHBoxLayout,
    QLineEdit,
    QPushButton,
    QWidget,
)

from database.database_tags import DatabaseTerms
from database.models import SessionLogDB, SettingsDB
from database.schema import load_objectives, objective_display_name, objective_sort_value

from .combo_alerts import combo_is_unset, lab_state_combo_alert_stylesheet, update_combo_alert, update_combo_alerts
from .adaptive_choice_selector import (
    AdaptiveChoiceSelector,
    objective_color,
    objective_short_label,
    objective_is_macro_profile,
    stain_color,
)
from .collapsible_section import CollapsibleSection
from .section_card import create_section_card
from .live_lab_raw import PendingRawCapture


class LiveLabPrepPanelMixin:
    """Microscope, Slide / prep and Notes cards of ``LiveLabTab`` and their combo helpers.

    Mixed into ``LiveLabTab``; the methods use the tab's widgets and state through ``self``.
    """

    # The contrast last used with each objective (JSON {objective: contrast}).
    SETTING_CONTRAST_BY_OBJECTIVE = "live_lab_contrast_by_objective"

    # Open/closed state of the collapsible sections, remembered across restarts.
    SETTING_PREP_SECTION_EXPANDED = "live_lab_prep_section_expanded"
    SETTING_NOTES_SECTION_EXPANDED = "live_lab_notes_section_expanded"

    def _build_microscope_selectors(self) -> None:
        """Build the objective and contrast selectors (``objective_combo``, ``contrast_combo``).

        They have no card since stage 2: they stay hidden in
        ``self.microscope_selectors`` as the runtime binding, and the
        capture-state bar over the viewer shows and drives them.
        """
        self.microscope_selectors = QWidget(self)
        self.microscope_selectors.hide()
        self.objective_combo = self._make_combo()
        self.objective_combo.setParent(self.microscope_selectors)
        self.objective_combo.set_unselected_border_visible(False)
        self.objective_combo.currentIndexChanged.connect(self._save_objective_selection)
        self.contrast_combo = self._build_term_combo("contrast")
        self.contrast_combo.setParent(self.microscope_selectors)

    def _build_slide_prep_card(self) -> None:
        """Build the collapsible Preparation section (mount, stain, condition, source) as ``self.slide_prep_group``."""
        self.slide_prep_group = CollapsibleSection(
            self.tr("Preparation"),
            QFormLayout,
            settings_key=self.SETTING_PREP_SECTION_EXPANDED,
            body_margins=(10, 12, 10, 10),
        )
        prep_form = self.slide_prep_group.body_layout
        prep_form.setSpacing(8)
        self.mount_combo = self._build_term_combo("mount")
        self.stain_combo = self._build_term_combo("stain")
        self.sample_combo = self._build_term_combo("sample")
        self.sample_source_combo = self._build_term_combo(
            "sample_source",
            pill_labels=DatabaseTerms.sample_source_compact_pills(),
            tooltips=DatabaseTerms.sample_source_compact_tooltips(),
        )
        self.sample_combo.setToolTip(self.tr("Specimen condition"))
        self.sample_source_combo.setToolTip(self.tr("Sample source"))
        prep_form.addRow(self.tr("Mount:"), self.mount_combo)
        prep_form.addRow(self.tr("Stain:"), self.stain_combo)
        prep_form.addRow(self.tr("Condition:"), self.sample_combo)
        prep_form.addRow(self.tr("Source:"), self.sample_source_combo)
        for combo in (self.mount_combo, self.stain_combo, self.sample_combo, self.sample_source_combo):
            # Chip rows: click to choose, click the chosen chip again to clear,
            # "+ N more" shows the rest in place. "Not set" has no chip.
            combo.set_chip_mode(clear_value="Not_set")
            combo.add_display_listener(self._update_prep_summary)
        self._update_prep_summary()

    def _prep_summary_fields(self) -> tuple[tuple[str, object, bool], ...]:
        """(field label, selector, flagged when unset) in the order the summary reads."""
        return (
            (self.tr("Mount"), getattr(self, "mount_combo", None), True),
            (self.tr("Condition"), getattr(self, "sample_combo", None), True),
            (self.tr("Stain"), getattr(self, "stain_combo", None), True),
            (self.tr("Source"), getattr(self, "sample_source_combo", None), False),
        )

    def _prep_summary(self) -> tuple[str, str]:
        """The Preparation summary line and its unset-value warning ("" when none)."""
        parts: list[str] = []
        unset: list[str] = []
        for label, combo, flagged in self._prep_summary_fields():
            if combo is None:
                continue
            if combo.is_mixed():
                parts.append(self.tr("{field}: mixed").format(field=label))
            elif combo_is_unset(combo):
                if flagged:
                    unset.append(label)
            else:
                parts.append(str(combo.itemText(combo.currentIndex()) or ""))
        alert = ""
        if len(unset) == 1:
            alert = self.tr("{field} not set").format(field=unset[0])
        elif unset:
            alert = self.tr("{count} not set").format(count=len(unset))
        return " · ".join(part for part in parts if part), alert

    def _update_prep_summary(self) -> None:
        section = getattr(self, "slide_prep_group", None)
        if not isinstance(section, CollapsibleSection):
            return
        text, alert = self._prep_summary()
        section.set_summary(text, alert=alert)

    def _build_notes_card(self) -> None:
        """Build the collapsible Notes section (session note, image note) as ``self.notes_group``."""
        self.notes_group = CollapsibleSection(
            self.tr("Notes"),
            QFormLayout,
            settings_key=self.SETTING_NOTES_SECTION_EXPANDED,
            body_margins=(10, 12, 10, 10),
        )
        notes_form = self.notes_group.body_layout
        notes_form.setSpacing(8)
        note_row = QWidget()
        note_row_layout = QHBoxLayout(note_row)
        note_row_layout.setContentsMargins(0, 0, 0, 0)
        note_row_layout.setSpacing(6)
        self.session_note_input = QLineEdit()
        self.session_note_input.setPlaceholderText(self.tr("Add a timestamped session note"))
        self.session_note_input.returnPressed.connect(self._add_session_note)
        note_row_layout.addWidget(self.session_note_input, 1)
        self.add_note_btn = QPushButton(self.tr("Add"))
        self.add_note_btn.clicked.connect(self._add_session_note)
        note_row_layout.addWidget(self.add_note_btn, 0)
        notes_form.addRow(self.tr("Note:"), note_row)
        # The session's notes and lab-state changes, newest first (stage 4).
        self.session_log_list = QListWidget()
        self.session_log_list.setObjectName("sessionLogList")
        self.session_log_list.setFocusPolicy(Qt.NoFocus)
        self.session_log_list.setSelectionMode(QAbstractItemView.NoSelection)
        self.session_log_list.setTextElideMode(Qt.ElideRight)
        self.session_log_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.session_log_list.setVisible(False)
        notes_form.addRow("", self.session_log_list)
        try:
            from .image_import_dialog import AutoSizingPlainTextEdit
        except Exception:
            AutoSizingPlainTextEdit = None
        if AutoSizingPlainTextEdit is not None:
            self.image_note_input = AutoSizingPlainTextEdit(self, min_lines=2, max_lines=6)
            self.image_note_input.setObjectName("labImageNoteInput")
            self.image_note_input.setPlaceholderText(self.tr("Optional note for the selected image"))
            self.image_note_input.textChanged.connect(self._on_image_note_text_changed)
            notes_form.addRow(self.tr("Image note:"), self.image_note_input)
            self._image_note_mixed = False
            self._image_note_loading = False
        self._update_notes_summary()

    # Session-log attributes shown as change lines in Notes (stage 4).
    _LOGGED_CHANGE_FIELDS = ("objective_name", "contrast", "mount_medium", "stain")
    # In a live session each capture carries its own objective and contrast,
    # so Notes leaves those changes out (the session log still has them);
    # offline sessions need them to match photos to the microscope state.
    _LIVE_UNLISTED_FIELDS = ("objective_name", "contrast")

    def _session_log_lines(self, *, refresh: bool = False) -> list[tuple[str, str]]:
        """(time, text) lines for Notes from the running session's log, oldest first.

        Manual notes, and changes of mount and stain, plus objective and
        contrast in offline sessions; the state logged when the session
        started is the baseline, not a change. Read from ``session_logs`` and cached per session; ``refresh``
        rereads (after a note or change is logged, or a session starts or stops).
        """
        session_id = str(getattr(self, "_session_id", None) or "")
        cached = getattr(self, "_session_log_cache", None)
        if not refresh and cached is not None and cached[0] == session_id:
            return cached[1]
        lines: list[tuple[str, str]] = []
        if session_id:
            try:
                events = SessionLogDB.get_events(session_id=session_id)
            except Exception:
                events = []
            current: dict[str, str | None] = {}
            shown: dict[str, str | None] = {}  # the logged display text of each current value
            objectives = load_objectives()
            for event in events:
                event_type = str(event.get("event_type") or "")
                time_text = self._note_time_text(event.get("recorded_at"))
                if event_type == "manual_note":
                    lines.append((time_text, " ".join(str(event.get("value") or "").split())))
                    continue
                attribute = str(event.get("attribute_name") or "")
                if event_type != "dropdown_change" or attribute not in self._LOGGED_CHANGE_FIELDS:
                    continue
                if attribute in self._LIVE_UNLISTED_FIELDS and str(event.get("session_kind") or "").lower() == "live":
                    continue
                value = event.get("value")
                metadata = event.get("metadata_json") or event.get("metadata") or {}
                metadata = metadata if isinstance(metadata, dict) else {}
                display = metadata.get("display_value")
                if attribute not in current:
                    current[attribute], shown[attribute] = value, display  # the starting state
                    continue
                if value == current[attribute]:
                    continue
                text = self.tr("{field} {old} → {new}").format(
                    field=self._change_field_text(attribute),
                    old=self._change_value_text(attribute, current[attribute], shown.get(attribute), objectives),
                    new=self._change_value_text(attribute, value, display, objectives),
                )
                if metadata.get("remembered"):
                    text = self.tr("{change} (remembered)").format(change=text)
                lines.append((time_text, text))
                current[attribute], shown[attribute] = value, display
        self._session_log_cache = (session_id, lines)
        return lines

    def _change_value_text(self, attribute: str, value, display, objectives: dict) -> str:
        if value in (None, ""):
            return self.tr("not set")
        if attribute == "objective_name":
            objective = objectives.get(str(value))
            if objective is None:
                # A removed objective: its logged name, never digits guessed from the key.
                return str(display or value)
            return objective_short_label(objective, str(value)) or str(value)
        category = {"contrast": "contrast", "mount_medium": "mount", "stain": "stain"}[attribute]
        return DatabaseTerms.translate(category, str(value))

    def _change_field_text(self, attribute: str) -> str:
        return {
            "objective_name": self.tr("Objective"),
            "contrast": self.tr("Contrast"),
            "mount_medium": self.tr("Mount"),
            "stain": self.tr("Stain"),
        }[attribute]

    @staticmethod
    def _note_time_text(recorded_at) -> str:
        text = str(recorded_at or "").strip()
        try:
            return datetime.fromisoformat(text).strftime("%H:%M")
        except ValueError:
            return text[11:16] if len(text) >= 16 else text

    def _notes_summary(self, *, refresh_log: bool = False) -> str:
        parts: list[str] = []
        lines = self._session_log_lines(refresh=refresh_log)
        if lines:
            time_text, text = lines[-1]
            if len(lines) == 1:
                parts.append(self.tr("1 line · {time} {text}").format(time=time_text, text=text))
            else:
                parts.append(
                    self.tr("{count} lines · last {time} {text}").format(count=len(lines), time=time_text, text=text)
                )
        widget = getattr(self, "image_note_input", None)
        # The note field keeps its last text when nothing is selected (a new
        # session, a cleared viewer), so it only counts while an image is.
        if widget is not None and self._has_selected_image():
            if getattr(self, "_image_note_mixed", False):
                parts.append(self.tr("Image: (Multiple notes)"))
            else:
                image_note = " ".join(str(widget.toPlainText() or "").split())
                if image_note:
                    parts.append(self.tr("Image: {note}").format(note=image_note))
        return " · ".join(parts) if parts else self.tr("No notes")

    def _has_selected_image(self) -> bool:
        if getattr(self, "_selected_session_image_id", None) is not None:
            return True
        current_pending = getattr(self, "_current_pending_raw_capture", None)
        if callable(current_pending) and current_pending() is not None:
            return True
        selected_keys = getattr(self, "_session_gallery_selected_keys", None)
        return bool(callable(selected_keys) and selected_keys())

    def _update_notes_summary(self, *, refresh_log: bool = False) -> None:
        section = getattr(self, "notes_group", None)
        if isinstance(section, CollapsibleSection):
            section.set_summary(self._notes_summary(refresh_log=refresh_log))
        log_list = getattr(self, "session_log_list", None)
        if log_list is not None:
            lines = self._session_log_lines()
            log_list.clear()
            for time_text, text in reversed(lines):  # newest first
                log_list.addItem(f"{time_text}  {text}")
                log_list.item(log_list.count() - 1).setToolTip(f"{time_text}  {text}")
            if lines:
                # As tall as its lines, up to six; the rest scroll.
                row = max(1, log_list.sizeHintForRow(0))
                frame = 2 * log_list.frameWidth()
                log_list.setFixedHeight(min(len(lines), 6) * row + frame + 2)
            log_list.setVisible(bool(lines))

    def _load_image_note_from_metadata(self, metadata: dict | None) -> None:
        widget = getattr(self, "image_note_input", None)
        if widget is None:
            return
        note = ""
        if isinstance(metadata, dict):
            note = str(metadata.get("notes") or metadata.get("image_note") or "")
        self._image_note_mixed = False
        self._image_note_loading = True
        try:
            widget.blockSignals(True)
            widget.setPlainText(note)
            widget.setPlaceholderText(self.tr("Optional note for the selected image"))
        finally:
            widget.blockSignals(False)
            self._image_note_loading = False
        self._update_notes_summary()

    def _load_image_note_from_metadatas(self, metadatas: list) -> None:
        widget = getattr(self, "image_note_input", None)
        if widget is None:
            return
        values = []
        for m in metadatas or []:
            if isinstance(m, dict):
                values.append(str(m.get("notes") or m.get("image_note") or ""))
            else:
                values.append("")
        distinct = []
        for v in values:
            if v not in distinct:
                distinct.append(v)
        self._image_note_loading = True
        try:
            widget.blockSignals(True)
            if len(distinct) <= 1:
                widget.setPlainText(distinct[0] if distinct else "")
                widget.setPlaceholderText(self.tr("Optional note for the selected image"))
                self._image_note_mixed = False
            else:
                widget.setPlainText("")
                widget.setPlaceholderText(self.tr("(Multiple notes)"))
                self._image_note_mixed = True
        finally:
            widget.blockSignals(False)
            self._image_note_loading = False
        self._update_notes_summary()

    def _on_image_note_text_changed(self) -> None:
        if getattr(self, "_image_note_loading", False):
            return
        widget = getattr(self, "image_note_input", None)
        if widget is None:
            return
        self._image_note_mixed = False
        self._update_notes_summary()
        text = str(widget.toPlainText() or "")
        captures = getattr(self, "_pending_raw_captures", []) or []
        selected_keys = set(self._session_gallery_selected_keys() or [])
        targets: list = []
        current_pending = self._current_pending_raw_capture()
        if not selected_keys and current_pending is not None:
            targets.append(current_pending)
        for capture in captures:
            if not isinstance(capture, PendingRawCapture):
                continue
            key = self._pending_raw_gallery_key(capture)
            if key in selected_keys or capture is current_pending:
                if capture not in targets:
                    targets.append(capture)
        for capture in targets:
            meta = dict(capture.lab_metadata or {})
            meta["notes"] = text
            capture.lab_metadata = meta

    def _make_combo(self):
        return AdaptiveChoiceSelector(self, compact=True)

    def _add_choice_item(
        self,
        combo,
        text: str,
        value,
        *,
        pill_text: str | None = None,
        tooltip: str | None = None,
        color: str | None = None,
    ) -> None:
        if isinstance(combo, AdaptiveChoiceSelector):
            combo.addItem(text, value, pillText=pill_text, tooltip=tooltip, color=color)
        else:
            combo.addItem(text, value)

    def _build_term_combo(
        self,
        category: str,
        *,
        pill_labels: dict[str, str] | None = None,
        tooltips: dict[str, str] | None = None,
    ):
        combo = self._make_combo()
        if category == "stain" and hasattr(combo, "set_unselected_border_visible"):
            combo.set_unselected_border_visible(False)
        adder = getattr(self, "_add_choice_item", None)
        values = DatabaseTerms.canonicalize_list(
            category,
            SettingsDB.get_list_setting(DatabaseTerms.setting_key(category), DatabaseTerms.default_values(category)),
        )
        for value in values:
            display = DatabaseTerms.translate(category, value)
            pill_override = (pill_labels or {}).get(value)
            if value == "Not_set":
                pill_text = "—"
            elif pill_override:
                pill_text = pill_override
            else:
                pill_text = display
            tooltip_override = (tooltips or {}).get(value)
            tooltip = tooltip_override or display
            if callable(adder):
                adder(
                    combo,
                    display,
                    value,
                    pill_text=pill_text,
                    color=stain_color(value) if category == "stain" else None,
                    tooltip=tooltip,
                )
            else:
                combo.addItem(display, value)
        combo.currentIndexChanged.connect(
            lambda _idx, cat=category, c=combo: self._remember_last_used_term(cat, c.currentData())
        )
        return combo

    def _refresh_term_combo(self, combo, category: str) -> None:
        if combo is None:
            return
        current_value = combo.currentData()
        combo.blockSignals(True)
        combo.clear()
        adder = getattr(self, "_add_choice_item", None)
        values = DatabaseTerms.canonicalize_list(
            category,
            SettingsDB.get_list_setting(DatabaseTerms.setting_key(category), DatabaseTerms.default_values(category)),
        )
        for value in values:
            display = DatabaseTerms.translate(category, value)
            if callable(adder):
                adder(
                    combo,
                    display,
                    value,
                    pill_text="—" if value == "Not_set" else display,
                    color=stain_color(value) if category == "stain" else None,
                    tooltip=display,
                )
            else:
                combo.addItem(display, value)
        index = combo.findData(current_value) if current_value is not None else -1
        if index < 0:
            index = 0 if combo.count() else -1
        if index >= 0:
            combo.setCurrentIndex(index)
        combo.blockSignals(False)

    def refresh_microscope_tag_preferences(self) -> None:
        for category, combo in (
            ("contrast", getattr(self, "contrast_combo", None)),
            ("mount", getattr(self, "mount_combo", None)),
            ("stain", getattr(self, "stain_combo", None)),
            ("sample", getattr(self, "sample_combo", None)),
            ("sample_source", getattr(self, "sample_source_combo", None)),
        ):
            self._refresh_term_combo(combo, category)
        self._update_lab_state_combo_alerts()

    def _restore_term_selection(self, combo, category: str) -> None:
        saved = DatabaseTerms.canonicalize(category, SettingsDB.get_setting(DatabaseTerms.last_used_key(category), ""))
        if not saved:
            return
        index = combo.findData(saved)
        if index >= 0:
            combo.setCurrentIndex(index)

    def _remember_last_used_term(self, category: str, value: str | None) -> None:
        if value:
            SettingsDB.set_setting(DatabaseTerms.last_used_key(category), str(value))

    def _lab_state_combo_alert_stylesheet(self) -> str:
        return lab_state_combo_alert_stylesheet(True)

    @staticmethod
    def _combo_is_unset(combo) -> bool:
        return combo_is_unset(combo)

    def _set_lab_state_combo_alert(self, combo, alert: bool) -> None:
        update_combo_alert(combo, alert)

    def _update_lab_state_combo_alerts(self, *_args) -> None:
        update_combo_alerts(
            (
                getattr(self, "objective_combo", None),
                getattr(self, "contrast_combo", None),
                getattr(self, "mount_combo", None),
                getattr(self, "stain_combo", None),
                getattr(self, "sample_combo", None),
            )
        )

    def _contrast_memory(self) -> dict[str, str]:
        try:
            data = json.loads(SettingsDB.get_setting(self.SETTING_CONTRAST_BY_OBJECTIVE, "") or "{}")
        except (TypeError, ValueError):
            return {}
        return {str(k): str(v) for k, v in data.items()} if isinstance(data, dict) else {}

    def _contrast_memory_applies(self) -> bool:
        """Only while choosing the state of the next capture: never while the side
        panel edits selected images, and never for a mixed selection."""
        if getattr(self, "_sidebar_binds_to_selection", False):
            return False
        return not (self.objective_combo.is_mixed() or self.contrast_combo.is_mixed())

    def _on_contrast_chosen(self, *_args) -> None:
        """An explicit contrast choice (bar click or shortcut), even an unchanged one."""
        self._remember_contrast_for_objective()

    def _remember_contrast_for_objective(self, *_args) -> None:
        if not self._contrast_memory_applies():
            return
        objective = self.objective_combo.selected_value()
        contrast = self.contrast_combo.selected_value()
        if not objective or not contrast or self.contrast_combo.is_unset_item(self.contrast_combo.currentIndex()):
            return
        memory = self._contrast_memory()
        if memory.get(str(objective)) == str(contrast):
            return
        memory[str(objective)] = str(contrast)
        SettingsDB.set_setting(self.SETTING_CONTRAST_BY_OBJECTIVE, json.dumps(memory, sort_keys=True))

    def _restore_contrast_for_objective(self, *_args) -> None:
        """Switching objective brings back the contrast last used with it."""
        if not self._contrast_memory_applies():
            return
        remembered = self._contrast_memory().get(str(self.objective_combo.selected_value() or ""))
        if not remembered or remembered == self.contrast_combo.selected_value():
            return
        if self.contrast_combo.findData(remembered) < 0:
            return  # hidden in Preferences since
        self._restoring_remembered_contrast = True
        try:
            self.contrast_combo.set_selected_value(remembered, emit=True)
        finally:
            self._restoring_remembered_contrast = False

    def _populate_objective_combo(self, *, keep_current: bool = False) -> None:
        """Refill the objective list and restore the stored choice.

        ``keep_current`` restores what is selected now instead (and a mixed
        selection), for a reload while working, such as after calibrating.
        """
        selector = self.objective_combo
        current = selector.selected_value() if keep_current else None
        mixed = list(getattr(selector, "_mixed_values", []) or []) if keep_current and selector.is_mixed() else []
        # Signals stay blocked while the list is refilled and the choice
        # restored: otherwise selecting the first new item saves it over the
        # stored choice before it is read (the P1 "objective forgotten" bug).
        blocked = selector.blockSignals(True)
        try:
            self._fill_objective_combo()
            if mixed:
                selector.set_mixed_values(mixed)
            elif current is not None and selector.findData(current) >= 0:
                selector.setCurrentIndex(selector.findData(current))
        finally:
            selector.blockSignals(blocked)
        self._update_lab_state_combo_alerts()
        refresh = getattr(self, "_refresh_viewer_objective_tag_from_current_state", None)
        if callable(refresh):
            refresh()

    def _fill_objective_combo(self) -> None:
        self.objective_combo.clear()
        adder = getattr(self, "_add_choice_item", None)
        objectives = load_objectives()
        rows = []
        for key, obj in objectives.items():
            if objective_is_macro_profile(obj, key):
                continue
            rows.append((key, obj))
        rows.sort(key=lambda item: objective_sort_value(item[1], item[0]))
        for key, obj in rows:
            label = objective_display_name(obj, key) or str(key)
            if callable(adder):
                adder(
                    self.objective_combo,
                    label,
                    key,
                    pill_text=objective_short_label(obj, key) or label,
                    color=objective_color(obj, key),
                    tooltip=label,
                )
            else:
                self.objective_combo.addItem(label, key)
        saved = str(SettingsDB.get_setting(self.SETTING_LAST_OBJECTIVE, "") or "").strip()
        if saved:
            index = self.objective_combo.findData(saved)
            if index >= 0:
                self.objective_combo.setCurrentIndex(index)

    def _save_objective_selection(self) -> None:
        objective_value = self._selected_combo_value(self.objective_combo)
        SettingsDB.set_setting(self.SETTING_LAST_OBJECTIVE, "" if objective_value is None else str(objective_value))
        refresh_viewer_objective_tag = getattr(self, "_refresh_viewer_objective_tag_from_current_state", None)
        if callable(refresh_viewer_objective_tag):
            refresh_viewer_objective_tag()

    def _add_session_note(self) -> None:
        note_text = str(self.session_note_input.text() or "").strip()
        if not note_text:
            return
        if not self.is_session_running():
            self._show_status(
                self.tr("Start a Live Lab session before adding notes."),
                tone="warning",
                timeout_ms=4000,
            )
            return
        self._log_session_event("manual_note", value=note_text, metadata={"note_length": len(note_text)})
        self.session_note_input.clear()
        self._update_notes_summary(refresh_log=True)
        self._show_status(
            self.tr("Added session note."),
            tone="success",
            timeout_ms=2500,
        )
