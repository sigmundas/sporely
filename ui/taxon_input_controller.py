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


_VERNACULAR_DISPLAY_ANNOTATED_LANGUAGES = frozenset({
    # Norwegian and Sámi variants that Stage 3B.2 keeps distinct — annotate
    # them in the completer popup so a user with multiple alternatives can
    # tell `hvit sprøsopp (nb)` from `kvit sprøsopp (nn)`. Every other
    # language renders bare, matching pre-existing test expectations.
    "nb", "nn", "se", "sma", "smj",
})


def _format_scientific_choice_display(suggestion: dict) -> str:
    """Stage 3B.3 disambiguation ladder — deterministic label for the
    scientific-name completer popup. Uses whatever fields the suggestion
    dict carries; never fabricates disambiguation from a rank heuristic.
    """
    name = str(suggestion.get("scientific_name") or "").strip()
    if not name:
        return ""
    parts: list[str] = [name]
    # 1. Authorship (rare in this DB but supported).
    authorship = str(suggestion.get("authorship") or "").strip()
    if authorship:
        parts.append(authorship)
    link_kind = suggestion.get("link_kind")
    canonical = str(suggestion.get("canonical_scientific_name") or "").strip()
    # 2. Alias / linked relation: show the canonical concept.
    if link_kind == "synonym_of_accepted" and canonical and canonical != name:
        parts.append(f"→ {canonical}")
    elif link_kind == "linked" and canonical and canonical != name:
        parts.append(f"↦ {canonical}")
    # 3. Source system when neither authorship nor alias disambiguates.
    source = str(suggestion.get("canonical_source_system") or "").strip()
    if len(parts) == 1 and source and source != "col_xr":
        # COL is the backbone default; annotate only non-default sources.
        parts.append(f"({source})")
    # 4. Family lineage, when still ambiguous.
    family = str(suggestion.get("family") or "").strip()
    if len(parts) == 1 and family:
        parts.append(f"family {family}")
    return "  ·  ".join(parts)


def format_common_name_choice_display(choice: TaxonChoice) -> str:
    """Render a vernacular suggestion for the completer popup.

    Only ``nb`` / ``nn`` / Sámi rows are annotated with a language-code
    suffix (so a user can distinguish alternatives for the same Sporely
    taxon). English and other single-language contexts render bare. The
    completer's completion role is ``Qt.UserRole`` which stores the raw
    name, so selection always writes the un-annotated string into the
    observation snapshot.
    """
    name = str(choice.common_name or "").strip()
    if not name:
        return ""
    lang = str(getattr(choice, "language_code", "") or "").strip().lower()
    if lang in _VERNACULAR_DISPLAY_ANNOTATED_LANGUAGES:
        return f"{name} ({lang})"
    return name


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
        scientific_name_input: QLineEdit | None = None,
        on_snapshot_invalidated: Callable[[], None] | None = None,
        on_snapshot_committed: Callable[[dict], None] | None = None,
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
        self._on_snapshot_invalidated = on_snapshot_invalidated
        self._on_snapshot_committed = on_snapshot_committed
        self.scientific_name_input = scientific_name_input
        # Stage 3B.3 selection-controlled snapshot.
        # None → no snapshot has been committed. Otherwise a tuple of
        # (genus, species, scientific_name, taxon_rank_snapshot,
        #  sporely_taxon_id, link_kind, canonical_scientific_name,
        #  canonical_rank).
        # Any manual divergence in `genus_input`, `species_input` or
        # `scientific_name_input` from THIS baseline invalidates the
        # snapshot: sporely_taxon_id / taxon_rank_snapshot / snapshot text
        # are all cleared. Retyping the identical string does NOT restore
        # identity — only another explicit suggestion selection can.
        self._committed_snapshot: dict | None = None
        # Suspend depth handles reentry AND load-time programmatic writes.
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

        # Stage 3B.3 scientific-name completer (optional widget). Its
        # dedicated model is a text-first popup that always shows the raw
        # scientific-name string (no language annotation), because the
        # observer picks the identification here, not a common name.
        self._scientific_model: _SuggestionModel | None = None
        self._scientific_completer: QCompleter | None = None
        if self.scientific_name_input is not None:
            self._scientific_model = _SuggestionModel(
                self, string_list_from_display=True,
            )
            self._scientific_completer = QCompleter(self._scientific_model, self)
            self._scientific_completer.setCaseSensitivity(Qt.CaseInsensitive)
            self._scientific_completer.setCompletionMode(QCompleter.PopupCompletion)
            self._scientific_completer.setCompletionRole(Qt.UserRole)
            self.scientific_name_input.setCompleter(self._scientific_completer)
            self._scientific_completer.activated[QModelIndex].connect(
                self.on_scientific_name_selected,
            )
            self._scientific_timer = QTimer(self)
            self._scientific_timer.setSingleShot(True)
            self._scientific_timer.timeout.connect(self.refresh_scientific_suggestions)
            self.scientific_name_input.textChanged.connect(
                self.on_scientific_name_text_changed,
            )
            self.scientific_name_input.installEventFilter(self)
        # Track invalidation on manual genus/species divergence (Stage 3B.3
        # rule 1). textChanged fires on every keystroke; when a suspend is
        # active (load-time programmatic writes) we treat the write as not
        # user-originated and skip invalidation.
        self.genus_input.textChanged.connect(self._on_structured_text_changed)
        self.species_input.textChanged.connect(self._on_structured_text_changed)

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
        # Stage 3B.2 contract: typing/editing custom vernacular text must
        # NEVER mutate taxonomy (`sporely_taxon_id`, genus, species,
        # species_guess, AI selection, external IDs). The previous behavior
        # silently cleared genus+species when the typed text had no matches
        # under the current taxon — this violated the contract. Only an
        # explicit completer selection (`on_vernacular_selected`) or Tab-
        # confirmed unique-match resolution (`on_vernacular_editing_finished`)
        # may change identity.
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

    # ------------------------------------------------------------------
    # Stage 3B.3 scientific-name snapshot API
    # ------------------------------------------------------------------

    def committed_snapshot(self) -> dict | None:
        """The last selection-committed snapshot, or None when the
        identification is unresolved. The dialog's save path reads this.
        """
        return dict(self._committed_snapshot) if self._committed_snapshot else None

    def load_committed_snapshot(self, snapshot: dict | None) -> None:
        """Programmatically restore a snapshot when loading an observation.

        Callers MUST also set genus/species/scientific inputs to matching
        values while inside :meth:`_suspended` so `_on_structured_text_changed`
        does not fire invalidation. This method itself does not touch the
        widgets — the dialog owns their state.
        """
        if not snapshot:
            self._committed_snapshot = None
            return
        # Coerce into a plain dict with the exact keys the controller uses.
        canon = str(snapshot.get("canonical_scientific_name") or "").strip()
        self._committed_snapshot = {
            "genus": str(snapshot.get("genus") or "").strip(),
            "species": str(snapshot.get("species") or "").strip(),
            "scientific_name": str(snapshot.get("scientific_name") or "").strip(),
            "taxon_rank_snapshot": snapshot.get("taxon_rank_snapshot"),
            "sporely_taxon_id": snapshot.get("sporely_taxon_id"),
            "link_kind": snapshot.get("link_kind"),
            "canonical_scientific_name": canon,
            "canonical_rank": str(snapshot.get("canonical_rank") or "").strip() or None,
        }

    def _invalidate_snapshot(self, *, reason: str) -> None:
        """Clear the committed snapshot and blank the scientific-name input
        + rank + Sporely id. Preserves genus, species, common_name, and
        every ai_selected_* field.

        Fired by manual divergence of any of the three editable scientific
        surfaces (`genus_input`, `species_input`, `scientific_name_input`).
        Called from `_on_structured_text_changed` and
        `on_scientific_name_text_changed` — both guarded by
        :meth:`_is_suspended` so load-time programmatic writes never
        invalidate.
        """
        if self._committed_snapshot is None:
            return
        self._committed_snapshot = None
        # Clear the scientific-name field only when the invalidation came
        # from a genus/species divergence — the scientific-name field
        # itself already reflects what the user is typing. Leaving it as
        # the user's typed text respects rule 4 (custom text remains, ID
        # doesn't).
        if reason == "structured_diverged" and self.scientific_name_input is not None:
            with self._suspended():
                self._set_text(self.scientific_name_input, "")
        if self._on_snapshot_invalidated is not None:
            try:
                self._on_snapshot_invalidated()
            except Exception:
                pass

    def _on_structured_text_changed(self, _text: str) -> None:
        if self._is_suspended():
            return
        if self._committed_snapshot is None:
            return
        current_genus = self._current_genus()
        current_species = self._current_species()
        if current_genus == self._committed_snapshot["genus"] \
                and current_species == self._committed_snapshot["species"]:
            return
        # A genus or species divergence invalidates identity — rule 1.
        self._invalidate_snapshot(reason="structured_diverged")

    def on_scientific_name_text_changed(self, text: str) -> None:
        if self._is_suspended():
            return
        if self.scientific_name_input is None:
            return
        cleaned = " ".join(str(text or "").strip().split())
        if self._committed_snapshot is not None \
                and cleaned == self._committed_snapshot["scientific_name"]:
            return
        # Every character divergence from the committed snapshot triggers
        # invalidation (rule 4 — no text-based rebinding). We do NOT try
        # to match against arbitrary DB rows here; only an explicit
        # completer selection may bind identity.
        if self._committed_snapshot is not None:
            self._invalidate_snapshot(reason="scientific_diverged")
        # Refresh the completer suggestions from the fresh prefix.
        if hasattr(self, "_scientific_timer"):
            self._schedule_refresh(self._scientific_timer)

    def refresh_scientific_suggestions(self) -> list[dict]:
        """Populate the scientific-name completer model with suggestions
        for the current input text. Returns the raw dicts."""
        if self._scientific_model is None or self.scientific_name_input is None:
            return []
        lookup = self.lookup
        prefix = self._current_scientific_text()
        if not prefix or not lookup:
            self._scientific_model.clear()
            return []
        suggestions = lookup.suggest_scientific_names(
            prefix=prefix, limit=self.max_suggestions,
        )
        # Rebuild the model.
        self._scientific_model.clear()
        for suggestion in suggestions:
            display = _format_scientific_choice_display(suggestion)
            item = QStandardItem(display)
            item.setData(str(suggestion["scientific_name"]), Qt.UserRole)
            item.setData(suggestion, ROLE_TAXON_CHOICE)
            self._scientific_model.appendRow(item)
        if self.auto_show_popup_on_focus and self._scientific_model.rowCount() \
                and self.scientific_name_input.hasFocus() \
                and self._scientific_completer is not None:
            self._scientific_completer.setCompletionPrefix("")
            self._scientific_completer.complete()
        return suggestions

    def _current_scientific_text(self) -> str:
        if self.scientific_name_input is None:
            return ""
        return " ".join(str(self.scientific_name_input.text() or "").strip().split())

    def commit_manual_resolution(
        self,
        *,
        sporely_taxon_id: int,
        scientific_name: str,
        taxon_rank_snapshot: str,
        genus: str | None = None,
        species: str | None = None,
        link_kind: str = "canonical",
        canonical_scientific_name: str | None = None,
        canonical_rank: str | None = None,
    ) -> bool:
        """Programmatically commit a snapshot for an identity resolved
        outside the completer picker (e.g. manual genus/species entry
        that the lookup service pins to a single canonical concept).

        Preserves the strict "identity binds only via explicit action"
        contract — the caller is responsible for having verified the
        resolution is unambiguous (see
        :meth:`TaxonLookupService.resolve_manual_scientific`). Returns
        ``True`` when a new snapshot was committed; ``False`` when the
        controller already holds the same identity, when the arguments
        are incomplete, or when the current text no longer matches the
        supplied genus/species (guarding against races between the
        editing_finished trigger and later user edits).
        """
        try:
            sporely_id_int = int(sporely_taxon_id)
        except (TypeError, ValueError):
            return False
        cleaned_sci = " ".join(str(scientific_name or "").strip().split())
        cleaned_rank = str(taxon_rank_snapshot or "").strip()
        if not cleaned_sci or not cleaned_rank:
            return False
        genus_text = self._clean_genus_text(genus if genus is not None else self._current_genus())
        species_text = self._clean_species_text(species if species is not None else self._current_species())
        if not genus_text or not species_text:
            return False
        # Guard against a race: the widgets must still hold the pair the
        # caller resolved. If the user has kept typing, the queued
        # editingFinished-driven resolution is stale and MUST NOT bind.
        current_genus = self._current_genus()
        current_species = self._current_species()
        if current_genus.casefold() != genus_text.casefold() \
                or current_species.casefold() != species_text.casefold():
            return False
        # No-op when the existing snapshot already pins this identity.
        existing = self._committed_snapshot
        if existing is not None \
                and int(existing.get("sporely_taxon_id") or 0) == sporely_id_int \
                and (existing.get("scientific_name") or "").strip() == cleaned_sci:
            return False
        self._committed_snapshot = {
            "genus": genus_text,
            "species": species_text,
            "scientific_name": cleaned_sci,
            "taxon_rank_snapshot": cleaned_rank,
            "sporely_taxon_id": sporely_id_int,
            "link_kind": link_kind or "canonical",
            "canonical_scientific_name": str(canonical_scientific_name or "").strip(),
            "canonical_rank": str(canonical_rank or "").strip() or None,
        }
        # Keep the scientific-name text widget in sync when it exists —
        # mirrors the on_scientific_name_selected code path. Wrapped in
        # suspension so this does not itself trigger invalidation.
        if self.scientific_name_input is not None \
                and self._current_scientific_text() != cleaned_sci:
            with self._suspended():
                self._set_text(self.scientific_name_input, cleaned_sci)
        if self._on_snapshot_committed is not None:
            try:
                self._on_snapshot_committed(dict(self._committed_snapshot))
            except Exception:
                pass
        return True

    def on_scientific_name_selected(self, index: QModelIndex) -> None:
        if self._is_suspended() or not index.isValid():
            return
        if self.scientific_name_input is None:
            return
        suggestion = index.data(ROLE_TAXON_CHOICE)
        if not isinstance(suggestion, dict):
            return
        scientific_name = str(suggestion.get("scientific_name") or "").strip()
        rank_snapshot = suggestion.get("taxon_rank_snapshot")
        sporely_id = suggestion.get("sporely_taxon_id")
        if not scientific_name or not rank_snapshot or sporely_id is None:
            return
        # Parse (genus, species) from the picked string using the same
        # bounded parser the suggestion source used — safe because the
        # picker only emits rows the parser accepted.
        from database.vernacular_db import parse_scientific_name_snapshot
        parsed = parse_scientific_name_snapshot(scientific_name)
        if parsed is None:
            return
        genus, species, _rank = parsed
        with self._suspended():
            self._set_text(self.genus_input, genus)
            self._set_text(self.species_input, species or "")
            self._set_text(self.scientific_name_input, scientific_name)
        self._committed_snapshot = {
            "genus": genus,
            "species": species or "",
            "scientific_name": scientific_name,
            "taxon_rank_snapshot": rank_snapshot,
            "sporely_taxon_id": int(sporely_id),
            "link_kind": suggestion.get("link_kind"),
            "canonical_scientific_name": str(suggestion.get("canonical_scientific_name") or "").strip(),
            "canonical_rank": str(suggestion.get("canonical_rank") or "").strip() or None,
        }
        if self._on_snapshot_committed is not None:
            try:
                self._on_snapshot_committed(dict(self._committed_snapshot))
            except Exception:
                pass
        self._sync_vernacular_after_taxon_change()
        self._notify_taxon_changed()

    def eventFilter(self, obj, event):
        event_type = event.type()
        if event_type == QEvent.FocusIn:
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
                self._open_vernacular_chooser()
                if _should_select_all_on_focus(event):
                    QTimer.singleShot(0, lambda widget=obj: widget.selectAll())
            elif obj is self.scientific_name_input \
                    and self.scientific_name_input is not None:
                self.refresh_scientific_suggestions()
                if _should_select_all_on_focus(event):
                    QTimer.singleShot(0, lambda widget=obj: widget.selectAll())
        elif event_type == QEvent.MouseButtonPress:
            if obj == self.vernacular_input and self.vernacular_input is not None:
                self._open_vernacular_chooser()
            elif obj is self.scientific_name_input \
                    and self.scientific_name_input is not None:
                self.refresh_scientific_suggestions()
        return False

    def _open_vernacular_chooser(self) -> None:
        """Force the vernacular completer popup to display every alternative
        for the currently resolved taxon, using an empty completion prefix
        so the user sees the full list without having to guess a starting
        letter. No-ops when the completer/model is unavailable."""
        if self._vernacular_model is None or self._vernacular_completer is None:
            return
        lookup = self.lookup
        genus = self._current_genus() or None
        species = self._current_species() or None
        if lookup and (genus or species):
            choices = lookup.suggest_common_names(
                prefix="", genus=genus, species=species, limit=self.max_suggestions
            )
            self._last_vernacular_query = ("", genus, species, self.max_suggestions)
            self._set_vernacular_model(choices, hide_on_exact=False, prefix="")
        else:
            self.refresh_vernacular_suggestions()
        if not self.auto_show_popup_on_focus:
            return
        if self._vernacular_model.rowCount() == 0:
            return
        # An empty completion prefix ensures QCompleter's default filter
        # matches every row — a real "chooser" experience regardless of
        # whether the user has typed anything.
        self._vernacular_completer.setCompletionPrefix("")
        self._vernacular_completer.complete()


__all__ = [
    "ROLE_TAXON_CHOICE",
    "TaxonInputController",
    "format_common_name_choice_display",
    "format_species_choice_display",
    "_format_scientific_choice_display",
]
