from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager

from PySide6.QtCore import QEvent, QModelIndex, QObject, QTimer, Qt, QStringListModel
from PySide6.QtGui import QStandardItem, QStandardItemModel
from PySide6.QtWidgets import QCompleter, QLineEdit

from database.taxon_lookup import TAXON_COMPLETER_LIMIT, TaxonChoice, TaxonLookupService


ROLE_TAXON_CHOICE = Qt.UserRole + 4


def format_species_choice_display(choice: TaxonChoice) -> str:
    return str(choice.species or "").strip()


def format_common_name_choice_display(choice: TaxonChoice) -> str:
    return str(choice.common_name or "").strip()


def _should_select_all_on_focus(event) -> bool:
    reason_getter = getattr(event, "reason", None)
    if callable(reason_getter):
        try:
            return reason_getter() != Qt.PopupFocusReason
        except Exception:
            return True
    return True


class _SuggestionModel(QStandardItemModel):
    """Small compatibility model for taxon suggestions.

    It keeps the richer item data needed by the controller, while still
    exposing ``stringList()`` and ``setStringList()`` for older call sites and
    tests that expect string-list-like behavior.
    """

    def __init__(self, parent: QObject | None = None, *, string_list_from_display: bool = False) -> None:
        super().__init__(parent)
        self._string_list_from_display = bool(string_list_from_display)

    def stringList(self) -> list[str]:
        values: list[str] = []
        for row in range(self.rowCount()):
            item = self.item(row, 0)
            if item is None:
                continue
            if self._string_list_from_display:
                value = str(item.text() or "").strip()
            else:
                value = str(item.data(Qt.UserRole) or item.text() or "").strip()
            if value:
                values.append(value)
        return values

    def setStringList(self, values: list[str]) -> None:
        self.clear()
        for value in values or []:
            text = str(value or "").strip()
            if not text:
                continue
            item = QStandardItem(text)
            item.setData(text, Qt.UserRole)
            self.appendRow(item)


class TaxonInputController(QObject):
    """Shared autocomplete/sync controller for genus, species, and common-name inputs."""

    def __init__(
        self,
        lookup: TaxonLookupService | None,
        genus_input: QLineEdit,
        species_input: QLineEdit,
        vernacular_input: QLineEdit | None = None,
        parent: QObject | None = None,
        *,
        max_suggestions: int = TAXON_COMPLETER_LIMIT,
        debounce_ms: int = 0,
        species_display_formatter: Callable[[TaxonChoice], str] | None = None,
        vernacular_display_formatter: Callable[[TaxonChoice], str] | None = None,
        species_item_customizer: Callable[[QStandardItem, TaxonChoice], None] | None = None,
        vernacular_item_customizer: Callable[[QStandardItem, TaxonChoice], None] | None = None,
        on_taxon_changed: Callable[[], None] | None = None,
        auto_show_popup_on_focus: bool = True,
    ) -> None:
        super().__init__(parent)
        self.lookup = lookup
        self.genus_input = genus_input
        self.species_input = species_input
        self.vernacular_input = vernacular_input
        self.max_suggestions = max(0, int(max_suggestions))
        self._debounce_ms = max(0, int(debounce_ms))
        self.auto_show_popup_on_focus = bool(auto_show_popup_on_focus)
        self._species_display_formatter = species_display_formatter or format_species_choice_display
        self._vernacular_display_formatter = vernacular_display_formatter or format_common_name_choice_display
        self._species_item_customizer = species_item_customizer
        self._vernacular_item_customizer = vernacular_item_customizer
        self._on_taxon_changed = on_taxon_changed
        self._suspend_depth = 0
        self._last_genus_signature: tuple[str, ...] = ()
        self._last_species_signature: tuple[tuple[str, str, str], ...] = ()
        self._last_vernacular_signature: tuple[tuple[str, str, str, str], ...] = ()
        self._last_genus_query: tuple[str, int] | None = None
        self._last_species_query: tuple[str, str, int] | None = None
        self._last_vernacular_query: tuple[str, str | None, str | None, int] | None = None

        self._genus_model = QStringListModel(self)
        self._species_model = _SuggestionModel(self)
        self._vernacular_model = _SuggestionModel(self, string_list_from_display=True) if vernacular_input is not None else None

        self._genus_completer = QCompleter(self._genus_model, self)
        self._genus_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._genus_completer.setCompletionMode(QCompleter.PopupCompletion)
        self.genus_input.setCompleter(self._genus_completer)
        self._genus_completer.activated[str].connect(self.on_genus_selected)

        self._species_completer = QCompleter(self._species_model, self)
        self._species_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._species_completer.setCompletionMode(QCompleter.PopupCompletion)
        self._species_completer.setCompletionRole(Qt.UserRole)
        self._species_completer.setFilterMode(Qt.MatchStartsWith)
        self.species_input.setCompleter(self._species_completer)
        self._species_completer.activated[QModelIndex].connect(self.on_species_selected)

        if self.vernacular_input is not None and self._vernacular_model is not None:
            self._vernacular_completer = QCompleter(self._vernacular_model, self)
            self._vernacular_completer.setCaseSensitivity(Qt.CaseInsensitive)
            self._vernacular_completer.setCompletionMode(QCompleter.PopupCompletion)
            self._vernacular_completer.setCompletionRole(Qt.UserRole)
            self.vernacular_input.setCompleter(self._vernacular_completer)
            self._vernacular_completer.activated[QModelIndex].connect(self.on_vernacular_selected)
        else:
            self._vernacular_completer = None

        self._genus_timer = QTimer(self)
        self._genus_timer.setSingleShot(True)
        self._genus_timer.timeout.connect(self.refresh_genus_suggestions)

        self._species_timer = QTimer(self)
        self._species_timer.setSingleShot(True)
        self._species_timer.timeout.connect(self.refresh_species_suggestions)

        self._vernacular_timer = QTimer(self)
        self._vernacular_timer.setSingleShot(True)
        self._vernacular_timer.timeout.connect(self.refresh_vernacular_suggestions)

        self.genus_input.textChanged.connect(self.on_genus_text_changed)
        self.species_input.textChanged.connect(self.on_species_text_changed)
        self.genus_input.editingFinished.connect(self.on_genus_editing_finished)
        self.species_input.editingFinished.connect(self.on_species_editing_finished)
        self.genus_input.installEventFilter(self)
        self.species_input.installEventFilter(self)

        if self.vernacular_input is not None:
            self.vernacular_input.textChanged.connect(self.on_vernacular_text_changed)
            self.vernacular_input.editingFinished.connect(self.on_vernacular_editing_finished)
            self.vernacular_input.installEventFilter(self)

    @property
    def genus_model(self) -> QStringListModel:
        return self._genus_model

    @property
    def species_model(self) -> QStandardItemModel:
        return self._species_model

    @property
    def vernacular_model(self) -> QStandardItemModel | None:
        return self._vernacular_model

    @property
    def genus_completer(self) -> QCompleter:
        return self._genus_completer

    @property
    def species_completer(self) -> QCompleter:
        return self._species_completer

    @property
    def vernacular_completer(self) -> QCompleter | None:
        return self._vernacular_completer

    @contextmanager
    def _suspended(self):
        self._suspend_depth += 1
        try:
            yield
        finally:
            self._suspend_depth = max(0, self._suspend_depth - 1)

    def _is_suspended(self) -> bool:
        return self._suspend_depth > 0

    @contextmanager
    def _blocked_signals(self, *widgets: QLineEdit | None):
        blocked: list[tuple[QLineEdit, bool]] = []
        for widget in widgets:
            if widget is None:
                continue
            try:
                blocked.append((widget, widget.blockSignals(True)))
            except Exception:
                continue
        try:
            yield
        finally:
            for widget, previous in reversed(blocked):
                try:
                    widget.blockSignals(previous)
                except Exception:
                    pass

    def _set_text(self, widget: QLineEdit | None, value: str) -> None:
        if widget is None:
            return
        text = str(value or "")
        if widget.text() == text:
            return
        with self._blocked_signals(widget):
            widget.setText(text)

    def _notify_taxon_changed(self) -> None:
        callback = self._on_taxon_changed
        if callable(callback):
            try:
                callback()
            except Exception:
                pass

    def _clean_genus_text(self, text: str | None) -> str:
        token = str(text or "").strip().split()
        return token[0].strip() if token else ""

    def _clean_species_text(self, text: str | None) -> str:
        return str(text or "").strip()

    def _current_genus(self) -> str:
        return self._clean_genus_text(self.genus_input.text())

    def _current_species(self) -> str:
        return self._clean_species_text(self.species_input.text())

    def _current_vernacular(self) -> str:
        if self.vernacular_input is None:
            return ""
        return str(self.vernacular_input.text() or "").strip()

    def _choice_signature(self, choice: TaxonChoice) -> tuple[str, str, str]:
        return (
            self._clean_genus_text(choice.genus),
            self._clean_species_text(choice.species),
            self._clean_species_text(choice.source),
        )

    def _set_genus_model(self, suggestions: list[str], *, hide_on_exact: bool = False, prefix: str = "") -> list[str]:
        values = [self._clean_genus_text(value) for value in suggestions if self._clean_genus_text(value)]
        if hide_on_exact and prefix:
            prefix_lower = prefix.casefold()
            if any(value.casefold() == prefix_lower for value in values):
                if self._genus_model.stringList():
                    self._genus_model.setStringList([])
                popup = self._genus_completer.popup()
                if popup:
                    popup.hide()
                return values
        if tuple(self._genus_model.stringList()) != tuple(values):
            self._genus_model.setStringList(values)
        return values

    def _set_species_model(self, choices: list[TaxonChoice], *, hide_on_exact: bool = False, prefix: str = "") -> list[str]:
        values = [self._clean_species_text(choice.species) for choice in choices if self._clean_species_text(choice.species)]
        if hide_on_exact and prefix:
            prefix_lower = prefix.casefold()
            if any(value.casefold() == prefix_lower for value in values):
                if self._species_model.rowCount():
                    self._species_model.clear()
                popup = self._species_completer.popup()
                if popup:
                    popup.hide()
                return values
        signatures: tuple[tuple[str, str, str], ...] = tuple(
            self._choice_signature(choice)
            for choice in choices
            if self._clean_species_text(choice.species)
        )
        if signatures != self._last_species_signature:
            self._species_model.clear()
            built_signatures: list[tuple[str, str, str]] = []
            for choice in choices:
                species = self._clean_species_text(choice.species)
                if not species:
                    continue
                item = QStandardItem(self._species_display_formatter(choice))
                item.setData(species, Qt.UserRole)
                item.setData(self._clean_genus_text(choice.genus), Qt.UserRole + 1)
                item.setData(species, Qt.UserRole + 2)
                item.setData(choice, ROLE_TAXON_CHOICE)
                if self._species_item_customizer is not None:
                    self._species_item_customizer(item, choice)
                self._species_model.appendRow(item)
                built_signatures.append(
                    (
                        self._clean_genus_text(choice.genus),
                        species,
                        self._clean_species_text(choice.source),
                    )
                )
            self._last_species_signature = tuple(built_signatures)
        return values

    def _set_vernacular_model(self, choices: list[TaxonChoice], *, hide_on_exact: bool = False, prefix: str = "") -> list[str]:
        if self._vernacular_model is None:
            return []
        values = [self._clean_species_text(choice.common_name) for choice in choices if self._clean_species_text(choice.common_name)]
        if hide_on_exact and prefix:
            prefix_lower = prefix.casefold()
            if any(value.casefold() == prefix_lower for value in values):
                if self._vernacular_model.rowCount():
                    self._vernacular_model.clear()
                popup = self._vernacular_completer.popup() if self._vernacular_completer else None
                if popup:
                    popup.hide()
                return values
        signatures: tuple[tuple[str, str, str, str], ...] = tuple(
            (
                self._clean_species_text(choice.common_name),
                self._clean_genus_text(choice.genus),
                self._clean_species_text(choice.species),
                self._clean_species_text(choice.source),
            )
            for choice in choices
            if self._clean_species_text(choice.common_name)
        )
        if signatures != self._last_vernacular_signature:
            self._vernacular_model.clear()
            built_signatures: list[tuple[str, str, str, str]] = []
            for choice in choices:
                name = self._clean_species_text(choice.common_name)
                if not name:
                    continue
                item = QStandardItem(self._vernacular_display_formatter(choice))
                item.setData(name, Qt.UserRole)
                item.setData(self._clean_genus_text(choice.genus), Qt.UserRole + 1)
                item.setData(self._clean_species_text(choice.species), Qt.UserRole + 2)
                item.setData(choice, ROLE_TAXON_CHOICE)
                if self._vernacular_item_customizer is not None:
                    self._vernacular_item_customizer(item, choice)
                self._vernacular_model.appendRow(item)
                built_signatures.append(
                    (
                        name,
                        self._clean_genus_text(choice.genus),
                        self._clean_species_text(choice.species),
                        self._clean_species_text(choice.source),
                    )
                )
            self._last_vernacular_signature = tuple(built_signatures)
        return values

    def _schedule_refresh(self, timer: QTimer) -> None:
        timer.start(self._debounce_ms)

    def refresh_genus_suggestions(self) -> list[str]:
        lookup = self.lookup
        text = self._current_genus()
        query_key = (text.casefold(), self.max_suggestions)
        if self._last_genus_query == query_key and self._genus_model.stringList():
            return list(self._genus_model.stringList())
        suggestions = lookup.suggest_genera(text, limit=self.max_suggestions) if lookup else []
        self._last_genus_query = query_key
        return self._set_genus_model(suggestions, hide_on_exact=True, prefix=text)

    def refresh_species_suggestions(self) -> list[str]:
        lookup = self.lookup
        genus = self._current_genus()
        prefix = self._current_species()
        query_key = (genus.casefold(), prefix.casefold(), self.max_suggestions)
        if self._last_species_query == query_key and self._species_model.rowCount():
            return [
                self._clean_species_text(item.data(Qt.UserRole)) if item is not None else ""
                for row in range(self._species_model.rowCount())
                if (item := self._species_model.item(row, 0)) is not None
            ]
        choices = lookup.suggest_species(genus, prefix, limit=self.max_suggestions) if lookup and genus else []
        self._last_species_query = query_key
        values = self._set_species_model(choices, hide_on_exact=True, prefix=prefix)
        if self.species_input.hasFocus() and values and self.auto_show_popup_on_focus and self._species_completer:
            self._species_completer.setCompletionPrefix(prefix)
            self._species_completer.complete()
        return values

    def refresh_vernacular_suggestions(self) -> list[str]:
        if self._vernacular_model is None:
            return []
        lookup = self.lookup
        genus = self._current_genus() or None
        species = self._current_species() or None
        prefix = self._current_vernacular()
        query_key = (prefix.casefold(), genus, species, self.max_suggestions)
        if self._last_vernacular_query == query_key and self._vernacular_model.rowCount():
            return [
                self._clean_species_text(item.data(Qt.UserRole)) if item is not None else ""
                for row in range(self._vernacular_model.rowCount())
                if (item := self._vernacular_model.item(row, 0)) is not None
            ]
        choices = lookup.suggest_common_names(prefix=prefix, genus=genus, species=species, limit=self.max_suggestions) if lookup else []
        self._last_vernacular_query = query_key
        values = self._set_vernacular_model(choices, hide_on_exact=False, prefix=prefix)
        if self.vernacular_input is not None and self.vernacular_input.hasFocus() and values and self.auto_show_popup_on_focus and self._vernacular_completer:
            self._vernacular_completer.setCompletionPrefix(prefix)
            self._vernacular_completer.complete()
        return values

    def _exact_taxon_choice(self) -> TaxonChoice | None:
        lookup = self.lookup
        genus = self._current_genus()
        species = self._current_species()
        if not lookup or not genus or not species:
            return None
        return lookup.resolve_scientific(genus, species)

    def _sync_vernacular_after_taxon_change(self) -> None:
        if self.vernacular_input is None or self._vernacular_model is None:
            return
        lookup = self.lookup
        if not lookup:
            return
        genus = self._current_genus()
        species = self._current_species()
        current = self._current_vernacular()

        if not genus or not species:
            if current:
                with self._suspended():
                    self._set_text(self.vernacular_input, "")
            self._last_vernacular_query = None
            self._set_vernacular_model([], prefix="", hide_on_exact=False)
            self._notify_taxon_changed()
            return

        resolved = self._exact_taxon_choice()
        if resolved is None:
            if current:
                with self._suspended():
                    self._set_text(self.vernacular_input, "")
            self._last_vernacular_query = None
            self._set_vernacular_model([], prefix="", hide_on_exact=False)
            self._notify_taxon_changed()
            return

        suggestions = lookup.suggest_common_names(prefix="", genus=resolved.genus, species=resolved.species, limit=self.max_suggestions)
        self._last_vernacular_query = ("", resolved.genus, resolved.species, self.max_suggestions)
        self._set_vernacular_model(suggestions, prefix="", hide_on_exact=False)

        current_matches = lookup.resolve_common_name(current, genus=resolved.genus, species=resolved.species) if current else []
        current_matches_taxon = any(
            self._clean_genus_text(choice.genus).casefold() == resolved.genus.casefold()
            and self._clean_species_text(choice.species).casefold() == resolved.species.casefold()
            for choice in current_matches
        )
        if current and current_matches_taxon:
            return

        best_choice = lookup.best_common_name_for_taxon(resolved.genus, resolved.species)
        new_value = self._clean_species_text(best_choice.common_name) if best_choice and best_choice.common_name else ""
        if current != new_value:
            with self._suspended():
                self._set_text(self.vernacular_input, new_value)
        self._notify_taxon_changed()

    def _clear_scientific_taxon_for_vernacular_search(self) -> None:
        genus = self._current_genus()
        species = self._current_species()
        if not genus and not species:
            return
        with self._suspended():
            if genus:
                self._set_text(self.genus_input, "")
            if species:
                self._set_text(self.species_input, "")
        self._genus_model.setStringList([])
        self._species_model.clear()
        self._vernacular_model.clear()
        self._last_genus_query = None
        self._last_species_query = None
        if self._genus_completer.popup():
            self._genus_completer.popup().hide()
        if self._species_completer.popup():
            self._species_completer.popup().hide()
        if self._vernacular_completer and self._vernacular_completer.popup():
            self._vernacular_completer.popup().hide()
        self._last_vernacular_query = None
        self._notify_taxon_changed()

    def sync_vernacular_after_taxon_change(self) -> None:
        self._sync_vernacular_after_taxon_change()

    def resolve_current_taxon_to_accepted(self) -> bool:
        lookup = self.lookup
        if not lookup:
            return False
        resolved = self._exact_taxon_choice()
        if resolved is None:
            return False
        genus = self._current_genus()
        species = self._current_species()
        if genus.casefold() == resolved.genus.casefold() and species.casefold() == resolved.species.casefold():
            return False
        with self._suspended():
            self._set_text(self.genus_input, resolved.genus)
            self._set_text(self.species_input, resolved.species)
        self._notify_taxon_changed()
        return True

    def on_genus_text_changed(self, text: str) -> None:
        if self._is_suspended():
            return
        if not self._clean_genus_text(text):
            self._genus_model.setStringList([])
            self._last_genus_query = None
            popup = self._genus_completer.popup()
            if popup:
                popup.hide()
            if self.species_input.text().strip():
                with self._suspended():
                    self._set_text(self.species_input, "")
            if self.vernacular_input is not None and self.vernacular_input.text().strip():
                with self._suspended():
                    self._set_text(self.vernacular_input, "")
            self._species_model.clear()
            self._last_species_query = None
            if self._vernacular_model is not None:
                self._vernacular_model.clear()
                self._last_vernacular_query = None
            return
        if self.genus_input.hasFocus() and self._current_species():
            with self._suspended():
                self._set_text(self.species_input, "")
            if self.vernacular_input is not None and self._current_vernacular():
                with self._suspended():
                    self._set_text(self.vernacular_input, "")
            self._species_model.clear()
            self._last_species_query = None
            if self._vernacular_model is not None:
                self._vernacular_model.clear()
                self._last_vernacular_query = None
        self._schedule_refresh(self._genus_timer)
        if not self._current_species():
            self._schedule_refresh(self._species_timer)
        self._sync_vernacular_after_taxon_change()

    def on_species_text_changed(self, text: str) -> None:
        if self._is_suspended():
            return
        genus = self._current_genus()
        if not genus:
            self._species_model.clear()
            self._last_species_query = None
            if self._vernacular_model is not None:
                self._vernacular_model.clear()
                self._last_vernacular_query = None
            return
        if not self._clean_species_text(text):
            self._schedule_refresh(self._species_timer)
        else:
            self._schedule_refresh(self._species_timer)
        self._sync_vernacular_after_taxon_change()

    def on_vernacular_text_changed(self, text: str) -> None:
        if self._is_suspended() or self._vernacular_model is None:
            return
        clean_text = self._clean_species_text(text)
        if not clean_text:
            self._schedule_refresh(self._vernacular_timer)
            return
        lookup = self.lookup
        if lookup and lookup.vernacular_db:
            genus = self._current_genus()
            species = self._current_species()
            if genus and species:
                matches = lookup.suggest_common_names(prefix=clean_text, genus=genus, species=species, limit=self.max_suggestions)
                if not matches:
                    self._clear_scientific_taxon_for_vernacular_search()
        self._schedule_refresh(self._vernacular_timer)

    def on_genus_editing_finished(self) -> None:
        if self._is_suspended():
            return
        if self._genus_completer.popup().isVisible():
            return
        self.resolve_current_taxon_to_accepted()
        self._sync_vernacular_after_taxon_change()
        if self._current_genus() and not self._current_species():
            self.refresh_species_suggestions()

    def on_species_editing_finished(self) -> None:
        if self._is_suspended():
            return
        if self._species_completer.popup().isVisible():
            return
        self.resolve_current_taxon_to_accepted()
        self._sync_vernacular_after_taxon_change()
        self.refresh_species_suggestions()

    def on_vernacular_editing_finished(self) -> None:
        if self._is_suspended() or self.vernacular_input is None:
            return
        lookup = self.lookup
        if not lookup:
            return
        name = self._current_vernacular()
        if not name:
            return
        current_genus = self._current_genus() or None
        current_species = self._current_species() or None
        matches = lookup.resolve_common_name(name, genus=current_genus, species=current_species)
        if not matches and (current_genus or current_species):
            matches = lookup.resolve_common_name(name)
        if not matches:
            return
        current_match = any(
            self._clean_genus_text(choice.genus).casefold() == (current_genus or "").casefold()
            and self._clean_species_text(choice.species).casefold() == (current_species or "").casefold()
            for choice in matches
            if current_genus and current_species
        )
        if len(matches) != 1 and not current_match:
            return
        choice = matches[0]
        with self._suspended():
            if choice.common_name and self._current_vernacular().casefold() != choice.common_name.casefold():
                self._set_text(self.vernacular_input, choice.common_name)
            if choice.species:
                if choice.genus and self._clean_genus_text(choice.genus).casefold() != self._current_genus().casefold():
                    self._set_text(self.genus_input, choice.genus)
                if self._clean_species_text(choice.species).casefold() != self._current_species().casefold():
                    self._set_text(self.species_input, choice.species)
        if choice.species:
            self._sync_vernacular_after_taxon_change()
        self._notify_taxon_changed()

    def on_genus_selected(self, genus: str) -> None:
        if self._is_suspended():
            return
        cleaned = self._clean_genus_text(genus)
        if cleaned and cleaned != self._current_genus():
            with self._suspended():
                self._set_text(self.genus_input, cleaned)
        if not self._current_species():
            self.refresh_species_suggestions()
        self._sync_vernacular_after_taxon_change()
        self._notify_taxon_changed()

    def on_species_selected(self, index: QModelIndex) -> None:
        if self._is_suspended() or not index.isValid():
            return
        choice = index.data(ROLE_TAXON_CHOICE)
        species = ""
        if isinstance(choice, TaxonChoice):
            species = self._clean_species_text(choice.species)
        if not species:
            species = self._clean_species_text(index.data(Qt.UserRole) or index.data(Qt.DisplayRole))
        if species and species != self._current_species():
            with self._suspended():
                self._set_text(self.species_input, species)
        if isinstance(choice, TaxonChoice) and choice.common_name and self.vernacular_input is not None and not self._current_vernacular():
            with self._suspended():
                self._set_text(self.vernacular_input, choice.common_name)
        self._sync_vernacular_after_taxon_change()
        self._notify_taxon_changed()

    def on_vernacular_selected(self, index: QModelIndex) -> None:
        if self._is_suspended() or self.vernacular_input is None or not index.isValid():
            return
        choice = index.data(ROLE_TAXON_CHOICE)
        if not isinstance(choice, TaxonChoice):
            return
        vernacular_text = self._clean_species_text(choice.common_name)
        if vernacular_text and vernacular_text != self._current_vernacular():
            with self._suspended():
                self._set_text(self.vernacular_input, vernacular_text)
        if choice.species:
            if choice.genus and self._clean_genus_text(choice.genus).casefold() != self._current_genus().casefold():
                with self._suspended():
                    self._set_text(self.genus_input, choice.genus)
            if self._clean_species_text(choice.species).casefold() != self._current_species().casefold():
                with self._suspended():
                    self._set_text(self.species_input, choice.species)
            self._sync_vernacular_after_taxon_change()
        self._notify_taxon_changed()

    def eventFilter(self, obj, event):
        if event.type() == QEvent.FocusIn:
            if obj == self.genus_input:
                self.refresh_genus_suggestions()
                if self.auto_show_popup_on_focus and self._genus_model.stringList():
                    self._genus_completer.complete()
                if _should_select_all_on_focus(event):
                    QTimer.singleShot(0, lambda widget=obj: widget.selectAll())
            elif obj == self.species_input:
                self.refresh_species_suggestions()
                if self.auto_show_popup_on_focus and self._species_model.rowCount():
                    self._species_completer.setCompletionPrefix(self._current_species())
                    self._species_completer.complete()
                if _should_select_all_on_focus(event):
                    QTimer.singleShot(0, lambda widget=obj: widget.selectAll())
            elif obj == self.vernacular_input and self.vernacular_input is not None:
                self.refresh_vernacular_suggestions()
                if self.auto_show_popup_on_focus and self._vernacular_model is not None and self._vernacular_model.rowCount():
                    self._vernacular_completer.setCompletionPrefix(self._current_vernacular())
                    self._vernacular_completer.complete()
                if _should_select_all_on_focus(event):
                    QTimer.singleShot(0, lambda widget=obj: widget.selectAll())
        return False


__all__ = [
    "ROLE_TAXON_CHOICE",
    "TaxonInputController",
    "format_common_name_choice_display",
    "format_species_choice_display",
]
