"""Reference Library manager dialog — desktop slice.

Standalone top-level dialog for viewing and editing the normalized
reference library (``reference_works`` → ``reference_taxon_treatments``
→ ``reference_measurement_sets``). It reuses the existing repositories
and the canonical snapshot service; it never touches the legacy
``reference_values`` table, ``ReferenceDB``, or any cloud/web/landing
code path.

Three-pane layout:

- Left: searchable table of publications.
- Middle: hierarchical tree of treatments + measurement sets for the
  selected work.
- Right: detail panel for the selected record + create/edit/attach
  actions.

Attachment is deliberately delegated to the parent (MainWindow) via the
``attach_requested(str, str)`` signal so the existing
``attach_with_status`` + plotability + warning-row + rollback contract
stays in exactly one place. The button that emits it is only exposed
when the dialog was constructed with a positive ``active_observation_id``
and the current selection is a measurement set.
"""
from __future__ import annotations

import json
from dataclasses import fields
from typing import Any, Iterable

from PySide6.QtCore import QObject, QThread, Qt, Signal, Slot
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QToolButton,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from database.reference_citation import (
    build_full_citation,
    build_observation_reference_snapshot,
    build_short_label,
)
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ObservationReferenceUseRepository,
    ReferenceLibraryError,
    ReferenceValidationError,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)
from database.reference_library_schema import (
    OBSERVATION_REFERENCE_ROLES,
    REFERENCE_WORK_TYPES,
)
from references.measurement_parser import parse_measurement_string


# --- Completeness hints ----------------------------------------------------
#
# The library no longer stores a manually-assigned verification badge or a
# per-work visibility scope. Instead the UI derives non-blocking hints
# about which bibliographic fields are still empty. These hints:
#
#   * are computed from the current field values,
#   * are not persisted anywhere,
#   * never block saving, attaching or plotting a reference.
#
# Public exposure of an attached reference is governed by the observation's
# own visibility and by the frozen ``observation_reference_uses.snapshot_json``
# — this predicate has no bearing on any of that.


def reference_work_completeness_hints(work: "ReferenceWork | dict") -> list[str]:
    """Return the ordered list of bibliographic fields missing from ``work``.

    Accepts either a :class:`ReferenceWork` instance or the plain dict a
    form uses while the record is being edited. The return value is a
    list of translated-friendly *English source* strings (the caller
    routes each through ``self.tr(...)`` for display); callers that
    only need a truthy/falsy signal can use ``bool(hints)`` — an empty
    list means "no missing fields", not "the record is verified".
    """
    def _pick(key: str) -> str:
        value = work.get(key) if isinstance(work, dict) else getattr(work, key, None)
        if value is None:
            return ""
        return str(value).strip()

    hints: list[str] = []
    if not _pick("title"):
        hints.append("missing title")
    # Authors are stored as JSON — treat "[]" and unparseable strings as
    # missing. Editors are NOT part of the required-completeness set.
    authors_raw = _pick("authors_json")
    if not authors_raw:
        hints.append("missing authors")
    else:
        try:
            parsed = json.loads(authors_raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = None
        if not isinstance(parsed, list) or not parsed:
            hints.append("missing authors")
    year_value = work.get("year") if isinstance(work, dict) else getattr(work, "year", None)
    if year_value in (None, ""):
        hints.append("missing year")
    # A publication needs *some* container-shaped context. The exact
    # field varies by type — journal for an article, publisher for a
    # book, etc. — so we accept any of container/publisher/url as a
    # sign that the operator has provided context.
    if not (_pick("container_title") or _pick("publisher") or _pick("url")):
        hints.append("missing publication/container information")
    return hints

# ``data_kind`` values the manager exposes for creation. ``parmasto`` is a
# known biometric expression kind that the desktop plot pipeline does not
# render yet; existing records remain viewable but the UI must not offer
# it as a new-record option in this slice.
_CREATABLE_DATA_KINDS: tuple[str, ...] = ("range", "summary", "raw_points")


# --- Small helpers ---------------------------------------------------------


def _empty_to_none(text: str | None) -> str | None:
    if text is None:
        return None
    stripped = str(text).strip()
    return stripped or None


def _parse_optional_float(text: str | None) -> float | None:
    value = _empty_to_none(text)
    if value is None:
        return None
    return float(value)


def _parse_optional_int(text: str | None) -> int | None:
    value = _empty_to_none(text)
    if value is None:
        return None
    return int(value)


def _format_optional(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return ("%g" % value)
    return str(value)


def _snapshot_measurement_details(
    work: ReferenceWork,
    treatment: TaxonTreatment,
    measurement_set: MeasurementSet,
) -> dict[str, Any]:
    """Compose the canonical measurement-set detail dict for display.

    Delegates to :func:`build_observation_reference_snapshot` so the
    detail panel shows exactly the same values the attachment snapshot
    would carry — no local formatting drift, no fabricated statistics.
    """
    return build_observation_reference_snapshot(work, treatment, measurement_set)


def _finite_positive(value: Any) -> bool:
    """Mirror ``references.reference_plotting._finite_positive`` — the
    plot hint must not disagree with the translator on which sets are
    drawable."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return False
    import math
    return math.isfinite(f) and f > 0.0


def _rectangle_drawable(lmin: Any, lmax: Any, wmin: Any, wmax: Any) -> bool:
    if not (
        _finite_positive(lmin)
        and _finite_positive(lmax)
        and _finite_positive(wmin)
        and _finite_positive(wmax)
    ):
        return False
    return float(lmax) > float(lmin) and float(wmax) > float(wmin)


def _measurement_set_is_plottable_hint(measurement_set: MeasurementSet) -> bool:
    """Live UI hint aligned with the translator's plotability rule:

    * ``raw_points`` requires at least one finite, strictly positive
      paired point;
    * ``range`` / ``summary`` require either a finite-positive L/W
      rectangle (core or exceptional) or a finite-positive L/W mean pair.

    Attachment-time plotability (:func:`references.reference_plotting.
    translate_observation_reference_use`) remains authoritative; this
    predicate is duplicated only to avoid a runtime import cycle and
    must be updated in lock-step with the translator's rule.
    """
    kind = str(measurement_set.data_kind or "").strip().lower()
    if kind == "raw_points":
        raw = measurement_set.raw_points_json
        if not raw:
            return False
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        if not isinstance(parsed, list) or not parsed:
            return False
        for point in parsed:
            if isinstance(point, dict):
                length = point.get("length")
                if length is None:
                    length = point.get("l")
                width = point.get("width")
                if width is None:
                    width = point.get("w")
                if _finite_positive(length) and _finite_positive(width):
                    return True
        return False
    core_ok = _rectangle_drawable(
        measurement_set.length_core_min,
        measurement_set.length_core_max,
        measurement_set.width_core_min,
        measurement_set.width_core_max,
    )
    ext_ok = _rectangle_drawable(
        measurement_set.length_min,
        measurement_set.length_max,
        measurement_set.width_min,
        measurement_set.width_max,
    )
    mean_ok = _finite_positive(measurement_set.length_mean) and _finite_positive(
        measurement_set.width_mean
    )
    return core_ok or ext_ok or mean_ok


class _CompletenessHintLabel(QLabel):
    """Renders a derived, non-blocking bibliographic-completeness hint.

    Consumes the list returned by :func:`reference_work_completeness_hints`
    and renders it as a small amber label ("missing title, missing year";
    hidden when the list is empty). The hint is a display artifact only —
    it is never persisted, never blocks saving/attaching, and never
    represents a shared or public-catalogue moderation signal.
    """

    _HINT_LABELS = {
        "missing title": ("Missing title",),
        "missing authors": ("Missing authors",),
        "missing year": ("Missing year",),
        "missing publication/container information": (
            "Missing publication/container information",
        ),
    }

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWordWrap(True)
        self.setStyleSheet("QLabel { color: #b45309; }")
        self.set_hints([])

    def set_hints(self, hints: list[str]) -> None:
        if not hints:
            self.setText("")
            self.setVisible(False)
            return
        translated: list[str] = []
        for key in hints:
            candidates = self._HINT_LABELS.get(key)
            if candidates:
                translated.append(self.tr(candidates[0]))
            else:
                translated.append(self.tr(key))
        self.setText(", ".join(translated))
        self.setVisible(True)


# --- Work / Treatment / Measurement forms ----------------------------------


# Publication-detail fields the form knows about; every non-basic, non-
# identifier field. Widgets are ALWAYS created for every entry here so that
# switching type never erases a hidden value — visibility only toggles.
_ALL_PUBLICATION_FIELDS: frozenset[str] = frozenset(
    {"container_title", "editors", "edition", "volume", "issue",
     "pages", "publisher", "place"}
)


# Which publication-detail fields are shown for each known work type. Any
# type not present in this map falls back to "show every publication field"
# (a general publication-details section for unknown types).
_PUBLICATION_FIELD_VISIBILITY: dict[str, frozenset[str]] = {
    "article": frozenset({"container_title", "volume", "issue", "pages"}),
    "book": frozenset({"edition", "editors", "publisher", "place"}),
    "chapter": frozenset(
        {"container_title", "editors", "pages", "publisher", "place"}
    ),
    "website": frozenset({"container_title", "publisher"}),
    "dataset": frozenset({"container_title", "publisher", "place"}),
}


# Type-aware label for the container-title field: "Journal" for articles,
# "Book title" for chapters/contributions, plain "Container title" for
# the rest.
def _container_title_label_for(work_type: str, tr_) -> str:
    key = str(work_type or "").strip().lower()
    if key == "article":
        return tr_("Journal / container title:")
    if key == "chapter":
        return tr_("Container / book title:")
    return tr_("Container title:")


class _PersonRow(QWidget):
    """Single row inside :class:`_PersonListEditor`.

    Presents Family, Given, Organization inputs plus move-up/move-down/
    remove buttons. The row itself carries no persistence logic — that is
    owned by the parent editor.
    """

    changed = Signal()
    remove_requested = Signal(object)
    move_requested = Signal(object, int)

    def __init__(
        self,
        parent: QWidget | None,
        *,
        family: str = "",
        given: str = "",
        organization: str = "",
    ) -> None:
        super().__init__(parent)
        self._build_ui()
        # Populate WITHOUT firing the changed signal — callers set the row's
        # dirty state explicitly.
        self.set_values(family=family, given=given, organization=organization)
        self.family_input.textEdited.connect(self._emit_changed)
        self.given_input.textEdited.connect(self._emit_changed)
        self.organization_input.textEdited.connect(self._emit_changed)

    def _build_ui(self) -> None:
        grid = QGridLayout(self)
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setHorizontalSpacing(6)
        grid.setVerticalSpacing(2)

        self.family_input = QLineEdit()
        self.family_input.setPlaceholderText(self.tr("Family name"))
        self.given_input = QLineEdit()
        self.given_input.setPlaceholderText(self.tr("Given names"))
        self.organization_input = QLineEdit()
        self.organization_input.setPlaceholderText(
            self.tr("Organization (optional)")
        )

        self.up_btn = QToolButton()
        self.up_btn.setText("▲")
        self.up_btn.setToolTip(self.tr("Move up"))
        self.up_btn.clicked.connect(
            lambda: self.move_requested.emit(self, -1)
        )
        self.down_btn = QToolButton()
        self.down_btn.setText("▼")
        self.down_btn.setToolTip(self.tr("Move down"))
        self.down_btn.clicked.connect(
            lambda: self.move_requested.emit(self, 1)
        )
        self.remove_btn = QToolButton()
        self.remove_btn.setText("✕")
        self.remove_btn.setToolTip(self.tr("Remove"))
        self.remove_btn.clicked.connect(
            lambda: self.remove_requested.emit(self)
        )

        grid.addWidget(self.family_input, 0, 0)
        grid.addWidget(self.given_input, 0, 1)
        grid.addWidget(self.organization_input, 0, 2)
        grid.addWidget(self.up_btn, 0, 3)
        grid.addWidget(self.down_btn, 0, 4)
        grid.addWidget(self.remove_btn, 0, 5)
        grid.setColumnStretch(0, 3)
        grid.setColumnStretch(1, 3)
        grid.setColumnStretch(2, 3)

    def set_values(
        self, *, family: str = "", given: str = "", organization: str = ""
    ) -> None:
        self.family_input.setText(family)
        self.given_input.setText(given)
        self.organization_input.setText(organization)

    def _emit_changed(self, *_args: Any) -> None:
        self.changed.emit()

    def to_dict(self) -> dict[str, str]:
        """Canonical JSON shape: family/given/literal keys, blanks omitted.

        ``literal`` is used for organization/institution names so the
        existing :mod:`database.reference_citation` formatter picks them up
        via :func:`_agent_label` / :func:`_agent_family_only`.
        """
        family = self.family_input.text().strip()
        given = self.given_input.text().strip()
        organization = self.organization_input.text().strip()
        result: dict[str, str] = {}
        if family:
            result["family"] = family
        if given:
            result["given"] = given
        if organization:
            result["literal"] = organization
        return result

    def is_empty(self) -> bool:
        return not any(
            (
                self.family_input.text().strip(),
                self.given_input.text().strip(),
                self.organization_input.text().strip(),
            )
        )


class _PersonListEditor(QWidget):
    """Ordered person-list editor for author/editor JSON fields.

    Loads existing canonical JSON (``[{"family": ..., "given": ...,
    "literal": ...}, ...]``) into human-friendly rows; serializes rows
    back to canonical JSON. Empty lists are supported. Malformed JSON is
    NOT silently discarded — a translated warning is exposed via
    :meth:`parse_error_message` and, if the user never edits the list,
    the original raw string is preserved verbatim on save.
    """

    changed = Signal()

    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        add_button_text: str | None = None,
    ) -> None:
        super().__init__(parent)
        self._rows: list[_PersonRow] = []
        self._dirty: bool = False
        self._original_raw: str | None = None
        self._parse_error: str | None = None
        self._add_button_text = add_button_text
        self._build_ui()

    def _build_ui(self) -> None:
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(4)

        self._rows_host = QWidget()
        self._rows_layout = QVBoxLayout(self._rows_host)
        self._rows_layout.setContentsMargins(0, 0, 0, 0)
        self._rows_layout.setSpacing(2)
        outer.addWidget(self._rows_host)

        self._empty_label = QLabel(self.tr("(no entries)"))
        self._empty_label.setStyleSheet("QLabel { color: #64748b; }")
        outer.addWidget(self._empty_label)

        self.warning_label = QLabel()
        self.warning_label.setStyleSheet("QLabel { color: #b45309; }")
        self.warning_label.setWordWrap(True)
        self.warning_label.setVisible(False)
        outer.addWidget(self.warning_label)

        row = QHBoxLayout()
        self.add_btn = QPushButton(
            self._add_button_text or self.tr("+ Add author")
        )
        self.add_btn.clicked.connect(self._on_add_clicked)
        row.addWidget(self.add_btn)
        row.addStretch(1)
        outer.addLayout(row)

        self._refresh_empty_state()

    # -- public helpers ---------------------------------------------------

    def add_row(
        self,
        *,
        family: str = "",
        given: str = "",
        organization: str = "",
        mark_dirty: bool = True,
    ) -> _PersonRow:
        row = _PersonRow(
            self._rows_host,
            family=family,
            given=given,
            organization=organization,
        )
        row.changed.connect(self._on_row_changed)
        row.remove_requested.connect(self._on_row_removed)
        row.move_requested.connect(self._on_row_move_requested)
        self._rows.append(row)
        self._rows_layout.addWidget(row)
        self._refresh_empty_state()
        if mark_dirty:
            self._on_row_changed()
        return row

    def load_entries(
        self, entries: Iterable[dict[str, Any]] | None
    ) -> None:
        """Clear and repopulate rows from a list of dicts.

        Does NOT mark the editor dirty. Used both when loading from an
        existing work and when re-populating after external edits.
        """
        self._clear_rows()
        for entry in (entries or []):
            if not isinstance(entry, dict):
                continue
            self.add_row(
                family=str(entry.get("family") or ""),
                given=str(entry.get("given") or ""),
                organization=str(entry.get("literal") or ""),
                mark_dirty=False,
            )
        self._dirty = False
        self._refresh_empty_state()

    def load_json(self, raw: str | None) -> None:
        """Load from a canonical JSON string.

        Parse failures are non-destructive: the editor stays empty, a
        translated warning is exposed via :attr:`warning_label`, and the
        original raw string is stashed. If the user does not modify the
        editor afterwards, :meth:`to_json` will return the original raw
        string verbatim so an unrelated edit to the work does not silently
        rewrite a value the form could not parse.
        """
        self._original_raw = raw
        self._parse_error = None
        self.warning_label.setVisible(False)
        self.warning_label.setText("")

        if raw is None or (isinstance(raw, str) and not raw.strip()):
            self._clear_rows()
            self._dirty = False
            self._refresh_empty_state()
            return

        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            self._parse_error = self.tr(
                "Existing value is not valid JSON and will be kept as-is "
                "until you edit the list: {error}"
            ).format(error=str(exc))
            self.warning_label.setText(self._parse_error)
            self.warning_label.setVisible(True)
            self._clear_rows()
            self._dirty = False
            self._refresh_empty_state()
            return

        if not isinstance(parsed, list):
            self._parse_error = self.tr(
                "Existing value is not a JSON list and will be kept as-is "
                "until you edit the list."
            )
            self.warning_label.setText(self._parse_error)
            self.warning_label.setVisible(True)
            self._clear_rows()
            self._dirty = False
            self._refresh_empty_state()
            return

        self._clear_rows()
        for entry in parsed:
            if isinstance(entry, dict):
                self.add_row(
                    family=str(entry.get("family") or ""),
                    given=str(entry.get("given") or ""),
                    organization=str(entry.get("literal") or ""),
                    mark_dirty=False,
                )
            elif isinstance(entry, str):
                self.add_row(
                    family=entry.strip(),
                    mark_dirty=False,
                )
            else:
                # Non-string, non-dict entry -> warn but keep others.
                self._parse_error = self.tr(
                    "One or more entries could not be understood; the "
                    "original value will be kept until you edit the list."
                )
                self.warning_label.setText(self._parse_error)
                self.warning_label.setVisible(True)
                self._clear_rows()
                self._dirty = False
                self._refresh_empty_state()
                return
        self._dirty = False
        self._refresh_empty_state()

    def entries(self) -> list[dict[str, str]]:
        return [row.to_dict() for row in self._rows if not row.is_empty()]

    def to_json(self) -> str:
        """Serialize to canonical JSON, preserving raw fallback if unread.

        When the initial :meth:`load_json` call could not parse the input
        AND the user has not since modified the editor, we return the
        stored raw string unchanged so a save on an unrelated field does
        not destroy the persisted value. Otherwise we serialize the rows.
        """
        if self._parse_error is not None and not self._dirty:
            return self._original_raw or "[]"
        return json.dumps(self.entries(), ensure_ascii=False)

    def is_dirty(self) -> bool:
        return self._dirty

    def parse_error_message(self) -> str | None:
        return self._parse_error

    # -- signal slots -----------------------------------------------------

    def _on_add_clicked(self) -> None:
        self.add_row(mark_dirty=True)

    def _on_row_changed(self) -> None:
        self._dirty = True
        self.changed.emit()
        # A user edit resolves the parse-error preservation contract —
        # from this point on we will serialize whatever the editor holds
        # rather than the untouched raw fallback.
        if self._parse_error is not None:
            # The warning stays visible until the editor is reloaded, so
            # the user still sees WHY the initial data was preserved.
            pass

    def _on_row_removed(self, row: _PersonRow) -> None:
        if row not in self._rows:
            return
        self._rows.remove(row)
        row.setParent(None)
        row.deleteLater()
        self._refresh_empty_state()
        self._on_row_changed()

    def _on_row_move_requested(self, row: _PersonRow, delta: int) -> None:
        try:
            idx = self._rows.index(row)
        except ValueError:
            return
        new_idx = idx + delta
        if new_idx < 0 or new_idx >= len(self._rows):
            return
        self._rows_layout.removeWidget(row)
        del self._rows[idx]
        self._rows.insert(new_idx, row)
        self._rows_layout.insertWidget(new_idx, row)
        self._on_row_changed()

    def _clear_rows(self) -> None:
        for row in list(self._rows):
            self._rows_layout.removeWidget(row)
            row.setParent(None)
            row.deleteLater()
        self._rows.clear()

    def _refresh_empty_state(self) -> None:
        self._empty_label.setVisible(not self._rows)


class _CollapsibleSection(QWidget):
    """Header + body pair that toggles body visibility.

    Used for the "Advanced citation details" section so the dialog can
    default to a compact laptop-sized layout while still exposing
    seldom-touched fields.
    """

    def __init__(
        self,
        title: str,
        parent: QWidget | None = None,
        *,
        expanded: bool = False,
    ) -> None:
        super().__init__(parent)
        self._button = QToolButton()
        self._button.setText(title)
        self._button.setCheckable(True)
        self._button.setChecked(expanded)
        self._button.setArrowType(Qt.DownArrow if expanded else Qt.RightArrow)
        self._button.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        self._button.setStyleSheet(
            "QToolButton { border: none; font-weight: 600; padding: 2px; }"
        )
        self._button.toggled.connect(self._on_toggled)

        self._body = QFrame()
        self._body.setFrameShape(QFrame.NoFrame)
        self._body_layout = QVBoxLayout(self._body)
        self._body_layout.setContentsMargins(12, 4, 4, 4)
        self._body.setVisible(expanded)

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(2)
        outer.addWidget(self._button)
        outer.addWidget(self._body)

    def body_layout(self) -> QVBoxLayout:
        return self._body_layout

    def set_expanded(self, expanded: bool) -> None:
        self._button.setChecked(expanded)

    def is_expanded(self) -> bool:
        return self._button.isChecked()

    def _on_toggled(self, checked: bool) -> None:
        self._body.setVisible(checked)
        self._button.setArrowType(Qt.DownArrow if checked else Qt.RightArrow)


class _ReferenceWorkForm(QDialog):
    """Human-facing bibliography form for a :class:`ReferenceWork`.

    Sections (top → bottom):

    1. Basic information — type, title, authors, year.
    2. Publication details — container/volume/issue/pages/edition/editors/
       publisher/place. Field visibility is driven by the selected type;
       every widget is always instantiated so that switching type does
       NOT erase any hidden value.
    3. Identifiers — DOI, ISBN, URL.
    4. Advanced citation details (collapsed by default) — manually
       overridden short label, citation key, language, full citation
       override, verification status, visibility.
    5. Live preview — short label + full citation from the canonical
       :mod:`database.reference_citation` service. Missing data produces
       an honestly incomplete preview, never a fabricated one. When the
       user has supplied a manual override the preview clearly labels
       the preview as "manual override".

    The dialog fits an ordinary laptop screen: the sections above sit
    inside a :class:`QScrollArea`, the buttons and preview stay pinned.
    """

    def __init__(
        self,
        parent: QWidget | None,
        *,
        work: ReferenceWork | None = None,
        persist_on_accept: bool = True,
    ) -> None:
        super().__init__(parent)
        self._work = work
        self._persist_on_accept = bool(persist_on_accept)
        self.setWindowTitle(
            self.tr("Edit reference work") if work is not None
            else self.tr("New reference work")
        )
        self.setModal(True)
        self.result_work: ReferenceWork | None = None

        self._all_input_widgets: list[QLineEdit] = []
        self._publication_row_labels: dict[str, QLabel] = {}
        self._publication_row_widgets: dict[str, QWidget] = {}

        self._build_ui()
        # Cap the dialog at a laptop-friendly size; the scroll area handles
        # any additional content growth.
        self.resize(720, 640)
        self.setMinimumWidth(560)
        self.setMinimumHeight(360)

        if work is not None:
            self._load(work)
        self._sync_publication_visibility()
        self._update_preview()

    # ----- UI construction ------------------------------------------------

    def _build_ui(self) -> None:
        outer = QVBoxLayout(self)
        outer.setContentsMargins(8, 8, 8, 8)
        outer.setSpacing(6)

        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        scroll_content = QWidget()
        scroll.setWidget(scroll_content)
        scroll_layout = QVBoxLayout(scroll_content)
        scroll_layout.setContentsMargins(4, 4, 4, 4)
        scroll_layout.setSpacing(10)
        outer.addWidget(scroll, 1)
        self._scroll_area = scroll

        scroll_layout.addWidget(self._build_basic_section())
        scroll_layout.addWidget(self._build_publication_section())
        scroll_layout.addWidget(self._build_identifiers_section())
        scroll_layout.addWidget(self._build_advanced_section())
        scroll_layout.addStretch(1)

        # Preview + error + buttons live outside the scroll area so they
        # remain visible on smaller screens.
        outer.addWidget(self._build_preview_section())

        self.error_label = QLabel()
        self.error_label.setStyleSheet("QLabel { color: #dc2626; }")
        self.error_label.setWordWrap(True)
        self.error_label.setVisible(False)
        outer.addWidget(self.error_label)

        buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel, Qt.Horizontal, self
        )
        buttons.accepted.connect(self._on_save)
        buttons.rejected.connect(self.reject)
        outer.addWidget(buttons)

    def _build_basic_section(self) -> QWidget:
        box = QGroupBox(self.tr("Basic information"))
        form = QFormLayout(box)

        self.type_combo = QComboBox()
        for value in sorted(REFERENCE_WORK_TYPES):
            self.type_combo.addItem(self._work_type_label(value), value)
        self.type_combo.currentIndexChanged.connect(self._on_type_changed)
        form.addRow(self.tr("Type:"), self.type_combo)

        self.title_input = QLineEdit()
        self.title_input.textChanged.connect(self._update_preview)
        self._all_input_widgets.append(self.title_input)
        form.addRow(self.tr("Title:"), self.title_input)

        self.authors_editor = _PersonListEditor(
            add_button_text=self.tr("+ Add author"),
        )
        self.authors_editor.changed.connect(self._update_preview)
        form.addRow(self.tr("Authors:"), self.authors_editor)

        self.year_input = QLineEdit()
        self.year_input.setPlaceholderText(self.tr("e.g. 1990"))
        self.year_input.textChanged.connect(self._update_preview)
        self._all_input_widgets.append(self.year_input)
        form.addRow(self.tr("Year:"), self.year_input)

        return box

    def _add_publication_row(
        self,
        form: QFormLayout,
        *,
        key: str,
        label: str,
        widget: QWidget,
    ) -> None:
        label_widget = QLabel(label)
        form.addRow(label_widget, widget)
        self._publication_row_labels[key] = label_widget
        self._publication_row_widgets[key] = widget

    def _build_publication_section(self) -> QWidget:
        box = QGroupBox(self.tr("Publication details"))
        form = QFormLayout(box)
        self._publication_form = form

        self.container_input = QLineEdit()
        self.container_input.textChanged.connect(self._update_preview)
        self._all_input_widgets.append(self.container_input)
        self._add_publication_row(
            form,
            key="container_title",
            label=self.tr("Container title:"),
            widget=self.container_input,
        )

        self.editors_editor = _PersonListEditor(
            add_button_text=self.tr("+ Add editor"),
        )
        self.editors_editor.changed.connect(self._update_preview)
        self._add_publication_row(
            form,
            key="editors",
            label=self.tr("Editors:"),
            widget=self.editors_editor,
        )

        self.edition_input = QLineEdit()
        self.edition_input.textChanged.connect(self._update_preview)
        self._all_input_widgets.append(self.edition_input)
        self._add_publication_row(
            form,
            key="edition",
            label=self.tr("Edition:"),
            widget=self.edition_input,
        )

        self.volume_input = QLineEdit()
        self.volume_input.textChanged.connect(self._update_preview)
        self._all_input_widgets.append(self.volume_input)
        self._add_publication_row(
            form,
            key="volume",
            label=self.tr("Volume:"),
            widget=self.volume_input,
        )

        self.issue_input = QLineEdit()
        self.issue_input.textChanged.connect(self._update_preview)
        self._all_input_widgets.append(self.issue_input)
        self._add_publication_row(
            form,
            key="issue",
            label=self.tr("Issue:"),
            widget=self.issue_input,
        )

        self.pages_input = QLineEdit()
        self.pages_input.textChanged.connect(self._update_preview)
        self._all_input_widgets.append(self.pages_input)
        self._add_publication_row(
            form,
            key="pages",
            label=self.tr("Pages:"),
            widget=self.pages_input,
        )

        self.publisher_input = QLineEdit()
        self.publisher_input.textChanged.connect(self._update_preview)
        self._all_input_widgets.append(self.publisher_input)
        self._add_publication_row(
            form,
            key="publisher",
            label=self.tr("Publisher:"),
            widget=self.publisher_input,
        )

        self.place_input = QLineEdit()
        self.place_input.textChanged.connect(self._update_preview)
        self._all_input_widgets.append(self.place_input)
        self._add_publication_row(
            form,
            key="place",
            label=self.tr("Place:"),
            widget=self.place_input,
        )

        return box

    def _build_identifiers_section(self) -> QWidget:
        box = QGroupBox(self.tr("Identifiers"))
        form = QFormLayout(box)

        self.doi_input = QLineEdit()
        self.doi_input.setPlaceholderText(self.tr("e.g. 10.1234/abcd"))
        self.doi_input.textChanged.connect(self._update_preview)
        self._all_input_widgets.append(self.doi_input)
        form.addRow(self.tr("DOI:"), self.doi_input)

        self.isbn_input = QLineEdit()
        self.isbn_input.setPlaceholderText(self.tr("digits or ISBN-10/13"))
        self._all_input_widgets.append(self.isbn_input)
        form.addRow(self.tr("ISBN:"), self.isbn_input)

        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText(self.tr("https://…"))
        self.url_input.textChanged.connect(self._update_preview)
        self._all_input_widgets.append(self.url_input)
        form.addRow(self.tr("URL:"), self.url_input)

        return box

    def _build_advanced_section(self) -> QWidget:
        section = _CollapsibleSection(
            self.tr("Advanced citation details"), expanded=False
        )
        body_layout = section.body_layout()
        self._advanced_section = section

        form = QFormLayout()
        form.setContentsMargins(0, 0, 0, 0)

        self.short_label_input = QLineEdit()
        self.short_label_input.setPlaceholderText(
            self.tr("Override — leave blank to use the generated value")
        )
        self.short_label_input.textChanged.connect(self._update_preview)
        self._all_input_widgets.append(self.short_label_input)
        form.addRow(
            self.tr("Short label override:"), self.short_label_input
        )

        self.citation_key_input = QLineEdit()
        self.citation_key_input.setPlaceholderText(
            self.tr("Optional short key, e.g. petersen-1990")
        )
        self._all_input_widgets.append(self.citation_key_input)
        form.addRow(self.tr("Citation key:"), self.citation_key_input)

        self.language_input = QLineEdit()
        self.language_input.setPlaceholderText(self.tr("ISO code, e.g. en"))
        self._all_input_widgets.append(self.language_input)
        form.addRow(self.tr("Language:"), self.language_input)

        self.citation_override_input = QPlainTextEdit()
        self.citation_override_input.setPlaceholderText(
            self.tr(
                "Override — leave blank to use the generated full citation"
            )
        )
        self.citation_override_input.setFixedHeight(60)
        self.citation_override_input.textChanged.connect(self._update_preview)
        form.addRow(
            self.tr("Full citation override:"), self.citation_override_input
        )

        body_layout.addLayout(form)
        return section

    def _build_preview_section(self) -> QWidget:
        box = QGroupBox(self.tr("Preview"))
        layout = QFormLayout(box)

        short_row = QHBoxLayout()
        short_row.setContentsMargins(0, 0, 0, 0)
        self.preview_short_label = QLabel()
        self.preview_short_label.setWordWrap(True)
        self.preview_short_label.setStyleSheet(
            "QLabel { font-weight: 600; }"
        )
        short_row.addWidget(self.preview_short_label, 1)
        self.preview_short_override_indicator = QLabel(
            self.tr("(manual override)")
        )
        self.preview_short_override_indicator.setStyleSheet(
            "QLabel { color: #b45309; }"
        )
        self.preview_short_override_indicator.setVisible(False)
        short_row.addWidget(self.preview_short_override_indicator)
        short_holder = QWidget()
        short_holder.setLayout(short_row)
        layout.addRow(self.tr("Short label:"), short_holder)

        full_row = QHBoxLayout()
        full_row.setContentsMargins(0, 0, 0, 0)
        self.preview_full_citation = QLabel()
        self.preview_full_citation.setWordWrap(True)
        full_row.addWidget(self.preview_full_citation, 1)
        self.preview_full_override_indicator = QLabel(
            self.tr("(manual override)")
        )
        self.preview_full_override_indicator.setStyleSheet(
            "QLabel { color: #b45309; }"
        )
        self.preview_full_override_indicator.setVisible(False)
        full_row.addWidget(self.preview_full_override_indicator)
        full_holder = QWidget()
        full_holder.setLayout(full_row)
        layout.addRow(self.tr("Full citation:"), full_holder)

        # Derived, non-blocking completeness hint. See
        # ``reference_work_completeness_hints`` for the rule.
        self.completeness_hints_label = _CompletenessHintLabel()
        layout.addRow(self.tr("Missing:"), self.completeness_hints_label)

        return box

    # ----- Type-aware helpers --------------------------------------------

    def _work_type_label(self, value: str) -> str:
        return {
            "book": self.tr("Book"),
            "article": self.tr("Article"),
            "chapter": self.tr("Chapter"),
            "website": self.tr("Website"),
            "dataset": self.tr("Dataset"),
            "other": self.tr("Other"),
        }.get(value, value)

    def _current_type(self) -> str:
        return str(self.type_combo.currentData() or "")

    def _visible_publication_fields(self) -> set[str]:
        work_type = self._current_type()
        allowed = _PUBLICATION_FIELD_VISIBILITY.get(work_type)
        if allowed is None:
            # Unknown or "other" -> show everything.
            return set(_ALL_PUBLICATION_FIELDS)
        return set(allowed)

    def _on_type_changed(self, *_args: Any) -> None:
        self._sync_publication_visibility()
        self._update_preview()

    def _sync_publication_visibility(self) -> None:
        visible = self._visible_publication_fields()
        for key, widget in self._publication_row_widgets.items():
            widget.setVisible(key in visible)
            label = self._publication_row_labels.get(key)
            if label is not None:
                label.setVisible(key in visible)

        # Update the container-title row label to reflect the type-aware
        # display name (e.g. "Journal" vs "Container / book title").
        container_label = self._publication_row_labels.get("container_title")
        if container_label is not None:
            container_label.setText(
                _container_title_label_for(self._current_type(), self.tr)
            )

    # ----- Load / collect -------------------------------------------------

    def _load(self, work: ReferenceWork) -> None:
        idx = self.type_combo.findData(work.type or "")
        if idx >= 0:
            self.type_combo.setCurrentIndex(idx)
        self.title_input.setText(work.title or "")
        self.short_label_input.setText(work.short_label or "")
        self.year_input.setText("" if work.year is None else str(work.year))
        # Editors first (initial signals are ignored by dirty tracking).
        self.authors_editor.load_json(work.authors_json)
        self.editors_editor.load_json(work.editors_json)
        self.citation_key_input.setText(work.citation_key or "")
        self.container_input.setText(work.container_title or "")
        self.edition_input.setText(work.edition or "")
        self.volume_input.setText(work.volume or "")
        self.issue_input.setText(work.issue or "")
        self.pages_input.setText(work.pages or "")
        self.publisher_input.setText(work.publisher or "")
        self.place_input.setText(work.place or "")
        self.doi_input.setText(work.doi or "")
        self.isbn_input.setText(work.isbn or "")
        self.url_input.setText(work.url or "")
        self.language_input.setText(work.language or "")
        self.citation_override_input.setPlainText(work.citation_override or "")

    def _collect(self) -> dict[str, Any]:
        return {
            "type": str(self.type_combo.currentData() or ""),
            "title": self.title_input.text().strip(),
            "short_label": self.short_label_input.text().strip(),
            "year": _parse_optional_int(self.year_input.text()),
            "authors_json": self.authors_editor.to_json(),
            "editors_json": self.editors_editor.to_json(),
            "citation_key": _empty_to_none(self.citation_key_input.text()),
            "container_title": _empty_to_none(self.container_input.text()),
            "edition": _empty_to_none(self.edition_input.text()),
            "volume": _empty_to_none(self.volume_input.text()),
            "issue": _empty_to_none(self.issue_input.text()),
            "pages": _empty_to_none(self.pages_input.text()),
            "publisher": _empty_to_none(self.publisher_input.text()),
            "place": _empty_to_none(self.place_input.text()),
            "doi": _empty_to_none(self.doi_input.text()),
            "isbn": _empty_to_none(self.isbn_input.text()),
            "url": _empty_to_none(self.url_input.text()),
            "language": _empty_to_none(self.language_input.text()),
            "citation_override": _empty_to_none(
                self.citation_override_input.toPlainText()
            ),
        }

    # ----- Preview -------------------------------------------------------

    def _preview_work(
        self, *, use_overrides: bool
    ) -> ReferenceWork | None:
        """Build an in-memory ReferenceWork from current inputs for the
        canonical citation service.

        Returns ``None`` when the current values would require the
        dataclass to validate — instead we swallow the year parse failure
        so the live preview stays honest ("year: blank") rather than
        blowing up on partial input.
        """
        try:
            year_value = _parse_optional_int(self.year_input.text())
        except (TypeError, ValueError):
            year_value = None

        short_label = (
            self.short_label_input.text().strip() if use_overrides else ""
        )
        override_text = (
            self.citation_override_input.toPlainText().strip()
            if use_overrides
            else ""
        )
        return ReferenceWork(
            id=self._work.id if self._work is not None else "",
            type=self._current_type() or "other",
            title=self.title_input.text().strip(),
            short_label=short_label,
            authors_json=self.authors_editor.to_json(),
            editors_json=self.editors_editor.to_json(),
            citation_key=_empty_to_none(self.citation_key_input.text()),
            container_title=_empty_to_none(self.container_input.text()),
            year=year_value,
            edition=_empty_to_none(self.edition_input.text()),
            publisher=_empty_to_none(self.publisher_input.text()),
            place=_empty_to_none(self.place_input.text()),
            volume=_empty_to_none(self.volume_input.text()),
            issue=_empty_to_none(self.issue_input.text()),
            pages=_empty_to_none(self.pages_input.text()),
            doi=_empty_to_none(self.doi_input.text()),
            isbn=_empty_to_none(self.isbn_input.text()),
            url=_empty_to_none(self.url_input.text()),
            language=_empty_to_none(self.language_input.text()),
            citation_override=override_text,
        )

    def _update_preview(self, *_args: Any) -> None:
        # Guard against callbacks firing during super().__init__ before
        # the preview widgets exist.
        if not hasattr(self, "preview_short_label"):
            return

        work = self._preview_work(use_overrides=True)
        if work is None:
            self.preview_short_label.setText("")
            self.preview_full_citation.setText("")
            self.preview_short_override_indicator.setVisible(False)
            self.preview_full_override_indicator.setVisible(False)
            return

        short_text = build_short_label(work) or ""
        full_text = build_full_citation(work) or ""
        self.preview_short_label.setText(short_text)
        self.preview_full_citation.setText(full_text)

        self.preview_short_override_indicator.setVisible(
            bool(self.short_label_input.text().strip())
        )
        self.preview_full_override_indicator.setVisible(
            bool(self.citation_override_input.toPlainText().strip())
        )
        # Completeness hints are derived from the current form values and
        # are non-blocking: an empty list simply hides the label.
        self.completeness_hints_label.set_hints(
            reference_work_completeness_hints(work)
        )

    # ----- Save / validate ----------------------------------------------

    def _first_invalid_widget(self) -> QWidget | None:
        """Return the widget to focus after a failed save."""
        if not self.title_input.text().strip():
            return self.title_input
        year_text = self.year_input.text().strip()
        if year_text:
            try:
                int(year_text)
            except ValueError:
                return self.year_input
        return None

    def _on_save(self) -> None:
        # Human-friendly local validation runs BEFORE repository mutation
        # so the first invalid field can be focused and the dialog stays
        # open on failure.
        invalid = self._first_invalid_widget()
        if invalid is self.title_input:
            self._show_error(self.tr("Title is required."))
            invalid.setFocus()
            return
        if invalid is self.year_input:
            self._show_error(
                self.tr("Year must be blank or a whole number.")
            )
            invalid.setFocus()
            return

        try:
            data = self._collect()
        except (TypeError, ValueError) as exc:
            self._show_error(str(exc))
            return
        if self._work is None:
            normalized_title = " ".join(data["title"].split()).casefold()
            def _first_author(value: str) -> tuple[str, str]:
                try:
                    people = json.loads(value or "[]")
                except (TypeError, ValueError, json.JSONDecodeError):
                    return ("", "")
                if not people or not isinstance(people[0], dict):
                    return ("", "")
                return (
                    str(people[0].get("family") or "").strip().casefold(),
                    str(people[0].get("given") or "").strip().casefold(),
                )

            proposed_author = _first_author(data["authors_json"])
            probable_duplicates = [
                candidate
                for candidate in ReferenceWorkRepository.search(
                    data["title"], limit=50
                )
                if " ".join(candidate.title.split()).casefold()
                == normalized_title
                and candidate.year == data["year"]
                and _first_author(candidate.authors_json) == proposed_author
            ]
            if probable_duplicates:
                answer = QMessageBox.question(
                    self,
                    self.tr("Possible duplicate reference"),
                    self.tr(
                        "A reference with the same title and year already "
                        "exists. Create another record anyway?"
                    ),
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No,
                )
                if answer != QMessageBox.Yes:
                    return
        try:
            if self._work is None and not self._persist_on_accept:
                self.result_work = ReferenceWork(id="", **data)
            elif self._work is None:
                new_work = ReferenceWork(id="", **data)
                self.result_work = ReferenceWorkRepository.create(new_work)
            else:
                self.result_work = ReferenceWorkRepository.update(
                    self._work.id, data
                )
        except (ReferenceValidationError, ReferenceLibraryError) as exc:
            self._show_error(str(exc))
            return
        self.accept()

    def _show_error(self, text: str) -> None:
        self.error_label.setText(text)
        self.error_label.setVisible(True)


# Public alias for callers outside this module that need to reuse the
# canonical publication editor (e.g. ReferenceAddDialog's "New
# publication…" affordance). The underscore-prefixed name remains for
# backwards compatibility with existing manager code and tests that
# import _ReferenceWorkForm directly.
ReferenceWorkEditor = _ReferenceWorkForm


class _TaxonTreatmentForm(QDialog):
    """Modal form for creating or editing a :class:`TaxonTreatment`."""

    def __init__(
        self,
        parent: QWidget | None,
        *,
        reference_work_id: str,
        treatment: TaxonTreatment | None = None,
    ) -> None:
        super().__init__(parent)
        self._reference_work_id = reference_work_id
        self._treatment = treatment
        self.setWindowTitle(
            self.tr("Edit taxon treatment") if treatment is not None
            else self.tr("New taxon treatment")
        )
        self.setModal(True)
        self.result_treatment: TaxonTreatment | None = None
        self._build_ui()
        if treatment is not None:
            self._load(treatment)

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.name_input = QLineEdit()
        form.addRow(self.tr("Name as published:"), self.name_input)

        self.taxon_input = QLineEdit()
        self.taxon_input.setPlaceholderText(self.tr("Optional taxon id"))
        form.addRow(self.tr("Taxon id:"), self.taxon_input)

        self.page_from_input = QLineEdit()
        form.addRow(self.tr("Page from:"), self.page_from_input)

        self.page_to_input = QLineEdit()
        form.addRow(self.tr("Page to:"), self.page_to_input)

        self.locator_input = QLineEdit()
        form.addRow(self.tr("Locator text:"), self.locator_input)

        self.notes_input = QPlainTextEdit()
        self.notes_input.setFixedHeight(70)
        form.addRow(self.tr("Notes:"), self.notes_input)

        layout.addLayout(form)

        self.error_label = QLabel()
        self.error_label.setStyleSheet("QLabel { color: #dc2626; }")
        self.error_label.setWordWrap(True)
        self.error_label.setVisible(False)
        layout.addWidget(self.error_label)

        buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel, Qt.Horizontal, self
        )
        buttons.accepted.connect(self._on_save)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _load(self, treatment: TaxonTreatment) -> None:
        self.name_input.setText(treatment.name_as_published or "")
        self.taxon_input.setText(treatment.taxon_id or "")
        self.page_from_input.setText(
            "" if treatment.page_from is None else str(treatment.page_from)
        )
        self.page_to_input.setText(
            "" if treatment.page_to is None else str(treatment.page_to)
        )
        self.locator_input.setText(treatment.locator_text or "")
        self.notes_input.setPlainText(treatment.treatment_notes or "")

    def _collect(self) -> dict[str, Any]:
        return {
            "name_as_published": self.name_input.text().strip(),
            "taxon_id": _empty_to_none(self.taxon_input.text()),
            "page_from": _parse_optional_int(self.page_from_input.text()),
            "page_to": _parse_optional_int(self.page_to_input.text()),
            "locator_text": _empty_to_none(self.locator_input.text()),
            "treatment_notes": _empty_to_none(self.notes_input.toPlainText()),
        }

    def _on_save(self) -> None:
        try:
            data = self._collect()
        except (TypeError, ValueError) as exc:
            self._show_error(str(exc))
            return
        try:
            if self._treatment is None:
                new = TaxonTreatment(
                    id="",
                    reference_work_id=self._reference_work_id,
                    **data,
                )
                self.result_treatment = TaxonTreatmentRepository.create(new)
            else:
                self.result_treatment = TaxonTreatmentRepository.update(
                    self._treatment.id, data
                )
        except (ReferenceValidationError, ReferenceLibraryError) as exc:
            self._show_error(str(exc))
            return
        self.accept()

    def _show_error(self, text: str) -> None:
        self.error_label.setText(text)
        self.error_label.setVisible(True)


class _MeasurementSetForm(QDialog):
    """Modal form for creating or editing a :class:`MeasurementSet`.

    Empty numeric inputs map to ``None`` — the dialog never fabricates
    zero or midpoint values. The ``data_kind`` combo restricts creation
    to plot-supported kinds (``range``/``summary``/``raw_points``); the
    ``parmasto`` kind is intentionally excluded for new records.
    """

    def __init__(
        self,
        parent: QWidget | None,
        *,
        taxon_treatment_id: str,
        measurement_set: MeasurementSet | None = None,
    ) -> None:
        super().__init__(parent)
        self._taxon_treatment_id = taxon_treatment_id
        self._measurement_set = measurement_set
        self.setWindowTitle(
            self.tr("Edit measurement set") if measurement_set is not None
            else self.tr("New measurement set")
        )
        self.setModal(True)
        self.result_set: MeasurementSet | None = None
        self._build_ui()
        if measurement_set is not None:
            self._load(measurement_set)
        self._sync_visibility()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.data_kind_combo = QComboBox()
        # When editing an existing parmasto set, keep it selectable so the
        # form does not silently switch kinds; but never offer parmasto
        # as a new choice.
        for value in _CREATABLE_DATA_KINDS:
            self.data_kind_combo.addItem(self._data_kind_label(value), value)
        if (
            self._measurement_set is not None
            and self._measurement_set.data_kind
            and self._measurement_set.data_kind not in _CREATABLE_DATA_KINDS
        ):
            self.data_kind_combo.addItem(
                self._data_kind_label(self._measurement_set.data_kind),
                self._measurement_set.data_kind,
            )
        self.data_kind_combo.currentIndexChanged.connect(
            lambda _idx: self._sync_visibility()
        )
        form.addRow(self.tr("Data kind:"), self.data_kind_combo)

        self.raw_text_input = QLineEdit()
        self.raw_text_input.setPlaceholderText(
            self.tr("e.g. (7.5–)8–10(–10.5) × 5–6(–6.5) µm")
        )
        raw_text_row = QHBoxLayout()
        raw_text_row.addWidget(self.raw_text_input, 1)
        self.parse_btn = QPushButton(self.tr("Parse expression"))
        self.parse_btn.clicked.connect(self._on_parse_clicked)
        raw_text_row.addWidget(self.parse_btn)
        raw_text_holder = QWidget()
        raw_text_holder.setLayout(raw_text_row)
        form.addRow(self.tr("Raw expression:"), raw_text_holder)

        self.length_min_input = QLineEdit()
        self.length_core_min_input = QLineEdit()
        self.length_core_max_input = QLineEdit()
        self.length_max_input = QLineEdit()
        self.length_mean_input = QLineEdit()
        self.width_min_input = QLineEdit()
        self.width_core_min_input = QLineEdit()
        self.width_core_max_input = QLineEdit()
        self.width_max_input = QLineEdit()
        self.width_mean_input = QLineEdit()
        self.q_min_input = QLineEdit()
        self.q_max_input = QLineEdit()
        self.q_mean_input = QLineEdit()
        self.sample_size_input = QLineEdit()
        self.specimen_count_input = QLineEdit()

        # Length row
        length_row = QHBoxLayout()
        length_row.addWidget(self.length_min_input)
        length_row.addWidget(self.length_core_min_input)
        length_row.addWidget(self.length_core_max_input)
        length_row.addWidget(self.length_max_input)
        length_holder = QWidget()
        length_holder.setLayout(length_row)
        self._length_row_label = QLabel(
            self.tr("Length min / core_min / core_max / max (µm):")
        )
        form.addRow(self._length_row_label, length_holder)

        self._length_mean_label = QLabel(self.tr("Length mean (µm):"))
        form.addRow(self._length_mean_label, self.length_mean_input)

        width_row = QHBoxLayout()
        width_row.addWidget(self.width_min_input)
        width_row.addWidget(self.width_core_min_input)
        width_row.addWidget(self.width_core_max_input)
        width_row.addWidget(self.width_max_input)
        width_holder = QWidget()
        width_holder.setLayout(width_row)
        self._width_row_label = QLabel(
            self.tr("Width min / core_min / core_max / max (µm):")
        )
        form.addRow(self._width_row_label, width_holder)

        self._width_mean_label = QLabel(self.tr("Width mean (µm):"))
        form.addRow(self._width_mean_label, self.width_mean_input)

        q_row = QHBoxLayout()
        q_row.addWidget(self.q_min_input)
        q_row.addWidget(self.q_mean_input)
        q_row.addWidget(self.q_max_input)
        q_holder = QWidget()
        q_holder.setLayout(q_row)
        self._q_row_label = QLabel(self.tr("Q min / mean / max:"))
        form.addRow(self._q_row_label, q_holder)

        self._sample_row_label = QLabel(self.tr("Sample size / specimens:"))
        sample_row = QHBoxLayout()
        sample_row.addWidget(self.sample_size_input)
        sample_row.addWidget(self.specimen_count_input)
        sample_holder = QWidget()
        sample_holder.setLayout(sample_row)
        form.addRow(self._sample_row_label, sample_holder)

        # Raw points editor: visible only when data_kind == raw_points.
        self.raw_points_input = QPlainTextEdit()
        self.raw_points_input.setPlaceholderText(
            self.tr(
                'JSON list, e.g. [{"length": 9.0, "width": 5.5}, '
                '{"length": 9.5, "width": 5.7}]'
            )
        )
        self.raw_points_input.setFixedHeight(120)
        self._raw_points_label = QLabel(self.tr("Raw points JSON:"))
        form.addRow(self._raw_points_label, self.raw_points_input)

        self.mount_medium_input = QLineEdit()
        form.addRow(self.tr("Mount medium:"), self.mount_medium_input)
        self.stain_input = QLineEdit()
        form.addRow(self.tr("Stain:"), self.stain_input)
        self.preparation_input = QLineEdit()
        form.addRow(self.tr("Preparation:"), self.preparation_input)
        self.method_input = QLineEdit()
        form.addRow(self.tr("Measurement method:"), self.method_input)

        self.notes_input = QPlainTextEdit()
        self.notes_input.setFixedHeight(60)
        form.addRow(self.tr("Notes:"), self.notes_input)

        layout.addLayout(form)

        self.error_label = QLabel()
        self.error_label.setStyleSheet("QLabel { color: #dc2626; }")
        self.error_label.setWordWrap(True)
        self.error_label.setVisible(False)
        layout.addWidget(self.error_label)

        buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel, Qt.Horizontal, self
        )
        buttons.accepted.connect(self._on_save)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _data_kind_label(self, value: str) -> str:
        return {
            "range": self.tr("Range"),
            "summary": self.tr("Summary"),
            "raw_points": self.tr("Raw points"),
            "parmasto": self.tr("Parmasto (read-only)"),
        }.get(value, value)

    def _load(self, ms: MeasurementSet) -> None:
        idx = self.data_kind_combo.findData(ms.data_kind or "")
        if idx >= 0:
            self.data_kind_combo.setCurrentIndex(idx)
        self.raw_text_input.setText(ms.raw_text or "")
        self.length_min_input.setText(_format_optional(ms.length_min))
        self.length_core_min_input.setText(_format_optional(ms.length_core_min))
        self.length_core_max_input.setText(_format_optional(ms.length_core_max))
        self.length_max_input.setText(_format_optional(ms.length_max))
        self.length_mean_input.setText(_format_optional(ms.length_mean))
        self.width_min_input.setText(_format_optional(ms.width_min))
        self.width_core_min_input.setText(_format_optional(ms.width_core_min))
        self.width_core_max_input.setText(_format_optional(ms.width_core_max))
        self.width_max_input.setText(_format_optional(ms.width_max))
        self.width_mean_input.setText(_format_optional(ms.width_mean))
        self.q_min_input.setText(_format_optional(ms.q_min))
        self.q_max_input.setText(_format_optional(ms.q_max))
        self.q_mean_input.setText(_format_optional(ms.q_mean))
        self.sample_size_input.setText(_format_optional(ms.sample_size))
        self.specimen_count_input.setText(_format_optional(ms.specimen_count))
        self.raw_points_input.setPlainText(ms.raw_points_json or "")
        self.mount_medium_input.setText(ms.mount_medium or "")
        self.stain_input.setText(ms.stain or "")
        self.preparation_input.setText(ms.preparation or "")
        self.method_input.setText(ms.measurement_method or "")
        self.notes_input.setPlainText(ms.notes or "")

    def _sync_visibility(self) -> None:
        kind = str(self.data_kind_combo.currentData() or "").strip().lower()
        raw_points_visible = kind == "raw_points"
        for widget in (
            self.raw_points_input,
            self._raw_points_label,
        ):
            widget.setVisible(raw_points_visible)
        # Length/width bounds are still meaningful for raw_points as an
        # aggregate summary in some datasets, but the plot-relevant flow
        # for raw_points relies on the raw list. Keep the numeric rows
        # hidden for raw_points to reduce clutter.
        for widget in (
            self.length_min_input,
            self.length_core_min_input,
            self.length_core_max_input,
            self.length_max_input,
            self.length_mean_input,
            self.width_min_input,
            self.width_core_min_input,
            self.width_core_max_input,
            self.width_max_input,
            self.width_mean_input,
            self.q_min_input,
            self.q_max_input,
            self.q_mean_input,
            self.sample_size_input,
            self.specimen_count_input,
            self._length_row_label,
            self._length_mean_label,
            self._width_row_label,
            self._width_mean_label,
            self._q_row_label,
            self._sample_row_label,
        ):
            widget.setVisible(not raw_points_visible)
        self.parse_btn.setVisible(not raw_points_visible and kind != "parmasto")

    def _on_parse_clicked(self) -> None:
        result = parse_measurement_string(self.raw_text_input.text())
        if not result.ok:
            self._show_error(
                self.tr(
                    "The expression could not be parsed. Keep the printed text "
                    "and enter the structured values manually."
                )
            )
            return

        def _set(widget: QLineEdit, value: float | int | None) -> None:
            widget.setText(_format_optional(value))

        _set(self.length_min_input, result.length.min)
        _set(self.length_core_min_input, result.length.p05)
        _set(self.length_core_max_input, result.length.p95)
        _set(self.length_max_input, result.length.max)
        _set(self.length_mean_input, result.length.p50)
        _set(self.width_min_input, result.width.min)
        _set(self.width_core_min_input, result.width.p05)
        _set(self.width_core_max_input, result.width.p95)
        _set(self.width_max_input, result.width.max)
        _set(self.width_mean_input, result.width.p50)
        # The normalized model has no Q core-bound columns. Preserve an
        # explicitly supplied Q range in q_min/q_max, preferring source
        # extremes when present and otherwise the printed core endpoints.
        _set(self.q_min_input, result.q.min if result.q.min is not None else result.q.p05)
        _set(self.q_max_input, result.q.max if result.q.max is not None else result.q.p95)
        _set(
            self.q_mean_input,
            result.q_mean if result.q_mean is not None else result.q.p50,
        )
        _set(self.sample_size_input, result.n)
        self.error_label.setVisible(False)

    def _collect(self) -> dict[str, Any]:
        kind = str(self.data_kind_combo.currentData() or "").strip().lower()
        payload: dict[str, Any] = {
            "character": "spore_size",
            "data_kind": kind,
            "raw_text": _empty_to_none(self.raw_text_input.text()),
            "mount_medium": _empty_to_none(self.mount_medium_input.text()),
            "stain": _empty_to_none(self.stain_input.text()),
            "preparation": _empty_to_none(self.preparation_input.text()),
            "measurement_method": _empty_to_none(self.method_input.text()),
            "notes": _empty_to_none(self.notes_input.toPlainText()),
        }
        # When editing an existing raw_points record we deliberately do NOT
        # blank the hidden aggregate statistics — the form's numeric rows
        # are hidden for raw_points, so writing None on save would silently
        # discard the user's previously-stored means/bounds. The fields
        # are only cleared when the user explicitly converts an existing
        # non-raw_points set INTO raw_points (a kind change), or on a
        # brand new record where those fields never existed.
        existing = self._measurement_set
        preserving_existing_raw_points_stats = (
            kind == "raw_points"
            and existing is not None
            and str(existing.data_kind or "").strip().lower() == "raw_points"
        )
        if kind == "raw_points":
            payload["raw_points_json"] = _empty_to_none(
                self.raw_points_input.toPlainText()
            )
            if preserving_existing_raw_points_stats:
                payload["length_min"] = existing.length_min
                payload["length_core_min"] = existing.length_core_min
                payload["length_core_max"] = existing.length_core_max
                payload["length_max"] = existing.length_max
                payload["length_mean"] = existing.length_mean
                payload["width_min"] = existing.width_min
                payload["width_core_min"] = existing.width_core_min
                payload["width_core_max"] = existing.width_core_max
                payload["width_max"] = existing.width_max
                payload["width_mean"] = existing.width_mean
                payload["q_min"] = existing.q_min
                payload["q_max"] = existing.q_max
                payload["q_mean"] = existing.q_mean
                payload["sample_size"] = existing.sample_size
                payload["specimen_count"] = existing.specimen_count
            else:
                # Fresh raw_points record (or user converted an existing
                # non-raw_points record). No pre-existing aggregate stats
                # for the raw list to overwrite; leave the columns NULL.
                payload["length_min"] = None
                payload["length_core_min"] = None
                payload["length_core_max"] = None
                payload["length_max"] = None
                payload["length_mean"] = None
                payload["width_min"] = None
                payload["width_core_min"] = None
                payload["width_core_max"] = None
                payload["width_max"] = None
                payload["width_mean"] = None
                payload["q_min"] = None
                payload["q_max"] = None
                payload["q_mean"] = None
                payload["sample_size"] = None
                payload["specimen_count"] = None
        else:
            payload["length_min"] = _parse_optional_float(self.length_min_input.text())
            payload["length_core_min"] = _parse_optional_float(
                self.length_core_min_input.text()
            )
            payload["length_core_max"] = _parse_optional_float(
                self.length_core_max_input.text()
            )
            payload["length_max"] = _parse_optional_float(self.length_max_input.text())
            payload["length_mean"] = _parse_optional_float(
                self.length_mean_input.text()
            )
            payload["width_min"] = _parse_optional_float(self.width_min_input.text())
            payload["width_core_min"] = _parse_optional_float(
                self.width_core_min_input.text()
            )
            payload["width_core_max"] = _parse_optional_float(
                self.width_core_max_input.text()
            )
            payload["width_max"] = _parse_optional_float(self.width_max_input.text())
            payload["width_mean"] = _parse_optional_float(
                self.width_mean_input.text()
            )
            payload["q_min"] = _parse_optional_float(self.q_min_input.text())
            payload["q_max"] = _parse_optional_float(self.q_max_input.text())
            payload["q_mean"] = _parse_optional_float(self.q_mean_input.text())
            payload["sample_size"] = _parse_optional_int(
                self.sample_size_input.text()
            )
            payload["specimen_count"] = _parse_optional_int(
                self.specimen_count_input.text()
            )
            # Symmetrical F-001 defense: editing a range/summary record
            # that happens to carry a persisted raw_points_json must not
            # silently drop that JSON. Preserve the prior JSON when the
            # data_kind is unchanged; only clear on an explicit kind
            # conversion (from raw_points into a non-raw kind).
            existing = self._measurement_set
            if (
                existing is not None
                and str(existing.data_kind or "").strip().lower() == kind
            ):
                payload["raw_points_json"] = existing.raw_points_json
            else:
                payload["raw_points_json"] = None
        return payload

    def _on_save(self) -> None:
        try:
            data = self._collect()
        except (TypeError, ValueError) as exc:
            self._show_error(str(exc))
            return
        try:
            if self._measurement_set is None:
                new = MeasurementSet(
                    id="",
                    taxon_treatment_id=self._taxon_treatment_id,
                    **data,
                )
                self.result_set = MeasurementSetRepository.create(new)
            else:
                self.result_set = MeasurementSetRepository.update(
                    self._measurement_set.id, data
                )
        except (ReferenceValidationError, ReferenceLibraryError) as exc:
            self._show_error(str(exc))
            return
        self.accept()

    def _show_error(self, text: str) -> None:
        self.error_label.setText(text)
        self.error_label.setVisible(True)


# --- Manager dialog --------------------------------------------------------


class _CurationSubmissionWorker(QObject):
    finished = Signal(object)
    failed = Signal(str)

    def __init__(self, client: object, measurement_set_id: str, version: str) -> None:
        super().__init__()
        self._client = client
        self._measurement_set_id = measurement_set_id
        self._version = version

    @Slot()
    def run(self) -> None:
        from database.curated_reference_forks import submit_personal_reference_for_curation
        try:
            result = submit_personal_reference_for_curation(
                self._client, self._measurement_set_id,
                attestation_version=self._version, rights_confirmed=True,
                curation_consent_confirmed=True,
            )
        except Exception as exc:
            self.failed.emit(str(exc))
            return
        self.finished.emit(result)


class ReferenceLibraryManagerDialog(QDialog):
    """Top-level Reference Library manager dialog.

    :param active_observation_id: When present, an "Attach to active
        observation" button is exposed for measurement-set selections.
        The dialog itself never mutates ``observation_reference_uses`` —
        it emits :attr:`attach_requested` and closes so MainWindow can
        route the attachment through its shared helper.
    """

    #: Emitted when the user requests an attachment.
    #: ``(measurement_set_id, role, observation_id)`` — role is a value
    #: from :data:`OBSERVATION_REFERENCE_ROLES`; ``observation_id`` is
    #: the observation id CAPTURED at manager-open time, so a rebind of
    #: the parent's active observation between open and click cannot
    #: silently redirect the attachment to a different observation.
    attach_requested = Signal(str, str, int)

    #: Emitted whenever a record was created/edited so external listeners
    #: (e.g. the attachment chooser) can refresh their state.
    library_changed = Signal()

    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        active_observation_id: int | None = None,
        cloud_client: object | None = None,
        sporely_taxon_id: int | None = None,
        curation_attestation_version: str | None = None,
        curation_attestation_text: str | None = None,
    ) -> None:
        super().__init__(parent)
        self._active_observation_id = (
            int(active_observation_id) if active_observation_id else None
        )
        self._cloud_client = cloud_client
        self._sporely_taxon_id = sporely_taxon_id
        self._curation_attestation_version = curation_attestation_version
        self._curation_attestation_text = curation_attestation_text
        self._submit_thread: QThread | None = None
        self._close_pending = False
        self.setWindowTitle(self.tr("Reference Library"))
        self.setModal(True)
        self.resize(1000, 640)

        self._works: list[ReferenceWork] = []
        self._current_work: ReferenceWork | None = None
        self._current_treatment: TaxonTreatment | None = None
        self._current_measurement_set: MeasurementSet | None = None

        self._build_ui()
        self.refresh_works()

    # --- UI construction ---

    def _build_ui(self) -> None:
        outer = QVBoxLayout(self)
        outer.setContentsMargins(10, 10, 10, 10)
        outer.setSpacing(8)

        splitter = QSplitter(Qt.Horizontal, self)
        splitter.setChildrenCollapsible(False)

        splitter.addWidget(self._build_works_pane())
        splitter.addWidget(self._build_hierarchy_pane())
        splitter.addWidget(self._build_detail_pane())
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 3)
        splitter.setStretchFactor(2, 4)
        outer.addWidget(splitter, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.Close, Qt.Horizontal, self)
        buttons.rejected.connect(self.reject)
        close_btn = buttons.button(QDialogButtonBox.Close)
        if close_btn is not None:
            close_btn.clicked.connect(self.reject)
        outer.addWidget(buttons)

    def _build_works_pane(self) -> QWidget:
        pane = QWidget()
        layout = QVBoxLayout(pane)
        layout.setContentsMargins(0, 0, 0, 0)

        layout.addWidget(QLabel(self.tr("Publications")))

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText(self.tr("Search by title, author or label"))
        self.search_input.textChanged.connect(self._on_search_changed)
        layout.addWidget(self.search_input)

        self.works_table = QTableWidget(0, 3, self)
        self.works_table.setHorizontalHeaderLabels(
            [self.tr("Short label"), self.tr("Year"), self.tr("Missing")]
        )
        self.works_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.works_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.works_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.works_table.verticalHeader().setVisible(False)
        header = self.works_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.works_table.itemSelectionChanged.connect(self._on_work_selected)
        layout.addWidget(self.works_table, 1)

        row = QHBoxLayout()
        self.new_work_btn = QPushButton(self.tr("New work…"))
        self.new_work_btn.clicked.connect(self._on_new_work_clicked)
        row.addWidget(self.new_work_btn)
        self.edit_work_btn = QPushButton(self.tr("Edit work…"))
        self.edit_work_btn.clicked.connect(self._on_edit_work_clicked)
        self.edit_work_btn.setEnabled(False)
        row.addWidget(self.edit_work_btn)
        row.addStretch(1)
        layout.addLayout(row)

        self._works_placeholder = QLabel(
            self.tr("No publications yet — use \"New work…\" to add one.")
        )
        self._works_placeholder.setWordWrap(True)
        self._works_placeholder.setStyleSheet("QLabel { color: #64748b; }")
        layout.addWidget(self._works_placeholder)
        return pane

    def _build_hierarchy_pane(self) -> QWidget:
        pane = QWidget()
        layout = QVBoxLayout(pane)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(QLabel(self.tr("Treatments and measurement sets")))
        self.hierarchy_tree = QTreeWidget()
        self.hierarchy_tree.setHeaderLabels(
            [self.tr("Name / kind"), self.tr("Revision")]
        )
        self.hierarchy_tree.setSelectionMode(QAbstractItemView.SingleSelection)
        self.hierarchy_tree.itemSelectionChanged.connect(self._on_hierarchy_selected)
        layout.addWidget(self.hierarchy_tree, 1)

        row = QHBoxLayout()
        self.new_treatment_btn = QPushButton(self.tr("New treatment…"))
        self.new_treatment_btn.clicked.connect(self._on_new_treatment_clicked)
        self.new_treatment_btn.setEnabled(False)
        row.addWidget(self.new_treatment_btn)
        self.new_set_btn = QPushButton(self.tr("New measurement set…"))
        self.new_set_btn.clicked.connect(self._on_new_measurement_set_clicked)
        self.new_set_btn.setEnabled(False)
        row.addWidget(self.new_set_btn)
        row.addStretch(1)
        layout.addLayout(row)
        return pane

    def _build_detail_pane(self) -> QWidget:
        pane = QWidget()
        layout = QVBoxLayout(pane)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(QLabel(self.tr("Details")))

        header_row = QHBoxLayout()
        self.detail_title = QLabel()
        self.detail_title.setStyleSheet("QLabel { font-weight: 600; }")
        header_row.addWidget(self.detail_title, 1)
        # Derived completeness hint replaces the old verification badge.
        # Hidden when the selected work has no missing fields; never
        # blocks any action.
        self.completeness_hint_label = _CompletenessHintLabel()
        header_row.addWidget(self.completeness_hint_label)
        layout.addLayout(header_row)

        self.detail_view = QPlainTextEdit()
        self.detail_view.setReadOnly(True)
        layout.addWidget(self.detail_view, 1)

        self.plot_hint_label = QLabel()
        self.plot_hint_label.setWordWrap(True)
        self.plot_hint_label.setStyleSheet("QLabel { color: #b45309; }")
        self.plot_hint_label.setVisible(False)
        layout.addWidget(self.plot_hint_label)

        self.edit_selected_btn = QPushButton(self.tr("Edit selected…"))
        self.edit_selected_btn.clicked.connect(self._on_edit_selected_clicked)
        self.edit_selected_btn.setEnabled(False)
        layout.addWidget(self.edit_selected_btn)

        self.delete_selected_btn = QPushButton(self.tr("Delete selected…"))
        self.delete_selected_btn.clicked.connect(self._on_delete_selected_clicked)
        self.delete_selected_btn.setEnabled(False)
        layout.addWidget(self.delete_selected_btn)

        self.copy_curated_btn = QPushButton(self.tr("Copy from public catalogue…"))
        self.copy_curated_btn.clicked.connect(self._on_copy_curated_clicked)
        self.copy_curated_btn.setEnabled(
            self._cloud_client is not None and self._sporely_taxon_id is not None
        )
        layout.addWidget(self.copy_curated_btn)

        self.submit_curation_btn = QPushButton(self.tr("Submit for curation…"))
        self.submit_curation_btn.clicked.connect(self._on_submit_curation_clicked)
        self.submit_curation_btn.setEnabled(False)
        layout.addWidget(self.submit_curation_btn)

        attach_row = QHBoxLayout()
        attach_row.addWidget(QLabel(self.tr("Role:")))
        self.role_combo = QComboBox()
        for role in ("compared", "supports_identification", "contradicts"):
            if role in OBSERVATION_REFERENCE_ROLES:
                self.role_combo.addItem(self._role_display_label(role), role)
        attach_row.addWidget(self.role_combo, 1)
        self.attach_btn = QPushButton(self.tr("Attach to active observation"))
        self.attach_btn.clicked.connect(self._on_attach_clicked)
        attach_row.addWidget(self.attach_btn)
        layout.addLayout(attach_row)
        self._update_attach_button_visibility()
        return pane

    # --- Data refresh helpers ---

    def refresh_works(self, *, select_id: str | None = None) -> None:
        query = self.search_input.text().strip() if hasattr(self, "search_input") else ""
        try:
            works = ReferenceWorkRepository.search(query or None, limit=200)
        except ReferenceLibraryError as exc:
            QMessageBox.warning(
                self,
                self.tr("Reference Library"),
                self.tr("Could not load publications: {error}").format(error=str(exc)),
            )
            works = []
        self._works = works
        self.works_table.setRowCount(0)
        for work in works:
            row = self.works_table.rowCount()
            self.works_table.insertRow(row)
            label_item = QTableWidgetItem(work.short_label or work.title or "")
            label_item.setData(Qt.UserRole, work.id)
            self.works_table.setItem(row, 0, label_item)
            year_item = QTableWidgetItem("" if work.year is None else str(work.year))
            self.works_table.setItem(row, 1, year_item)
            # Third column: derived, non-blocking completeness hint. Empty
            # cell means "no missing fields"; a non-zero count is a soft
            # nudge and never gates any action.
            hint_count = len(reference_work_completeness_hints(work))
            hint_item = QTableWidgetItem(
                "" if hint_count == 0 else str(hint_count)
            )
            if hint_count:
                hint_item.setForeground(QColor("#b45309"))
                hint_item.setToolTip(
                    self.tr("Missing bibliographic fields (derived).")
                )
            self.works_table.setItem(row, 2, hint_item)
        self._works_placeholder.setVisible(not works)
        if select_id:
            for row in range(self.works_table.rowCount()):
                item = self.works_table.item(row, 0)
                if item and item.data(Qt.UserRole) == select_id:
                    self.works_table.selectRow(row)
                    break
        else:
            self._current_work = None
            self._clear_hierarchy()
            self._clear_detail()

    def _completeness_hints_for(self, work: ReferenceWork) -> list[str]:
        """Public accessor for tests + subclasses. Returns the derived,
        non-persisted list of missing bibliographic fields for a work."""
        return reference_work_completeness_hints(work)

    def _add_hierarchy_placeholder(self, text: str) -> None:
        """Insert a disabled placeholder item into the hierarchy tree so
        the empty state is visible instead of an unlabelled blank pane."""
        item = QTreeWidgetItem([text, ""])
        item.setFlags(Qt.ItemIsEnabled)  # visible, not selectable
        item.setForeground(0, QColor("#64748b"))
        self.hierarchy_tree.addTopLevelItem(item)

    def _refresh_hierarchy_for_current_work(
        self, *, select_treatment_id: str | None = None,
        select_set_id: str | None = None,
    ) -> None:
        self.hierarchy_tree.clear()
        if self._current_work is None:
            self._add_hierarchy_placeholder(
                self.tr("Select a publication to see its treatments.")
            )
            return
        try:
            treatments = TaxonTreatmentRepository.list_for_work(
                self._current_work.id
            )
        except ReferenceLibraryError as exc:
            QMessageBox.warning(
                self,
                self.tr("Reference Library"),
                self.tr("Could not load treatments: {error}").format(error=str(exc)),
            )
            treatments = []
        if not treatments:
            self._add_hierarchy_placeholder(
                self.tr("No treatments yet — use \"New treatment…\" to add one.")
            )
            return
        selection_target = None
        for treatment in treatments:
            t_item = QTreeWidgetItem(
                [
                    treatment.name_as_published or self.tr("(unnamed treatment)"),
                    str(treatment.revision or 1),
                ]
            )
            t_item.setData(0, Qt.UserRole, ("treatment", treatment.id))
            self.hierarchy_tree.addTopLevelItem(t_item)
            try:
                sets = MeasurementSetRepository.list_for_treatment(treatment.id)
            except ReferenceLibraryError:
                sets = []
            for ms in sets:
                s_item = QTreeWidgetItem(
                    [
                        f"{ms.data_kind} — {ms.raw_text or ms.id}",
                        str(ms.revision or 1),
                    ]
                )
                s_item.setData(0, Qt.UserRole, ("measurement_set", ms.id))
                t_item.addChild(s_item)
                if select_set_id and ms.id == select_set_id:
                    selection_target = s_item
            if selection_target is None and select_treatment_id == treatment.id:
                selection_target = t_item
            t_item.setExpanded(True)
        if selection_target is not None:
            self.hierarchy_tree.setCurrentItem(selection_target)

    # --- Selection slots ---

    def _on_search_changed(self, _text: str) -> None:
        self.refresh_works()

    def _on_work_selected(self) -> None:
        rows = self.works_table.selectionModel().selectedRows() if self.works_table.selectionModel() else []
        if not rows:
            self._current_work = None
            self.edit_work_btn.setEnabled(False)
            self.new_treatment_btn.setEnabled(False)
            self.new_set_btn.setEnabled(False)
            self.delete_selected_btn.setEnabled(False)
            self._clear_hierarchy()
            self._clear_detail()
            return
        row = rows[0].row()
        item = self.works_table.item(row, 0)
        work_id = item.data(Qt.UserRole) if item else None
        work = next((w for w in self._works if w.id == work_id), None)
        self._current_work = work
        self._current_treatment = None
        self._current_measurement_set = None
        self.edit_work_btn.setEnabled(work is not None)
        self.new_treatment_btn.setEnabled(work is not None)
        self.new_set_btn.setEnabled(False)
        self.delete_selected_btn.setEnabled(work is not None)
        self._refresh_hierarchy_for_current_work()
        if work is not None:
            self._render_work_detail(work)
        self._update_attach_button_visibility()

    def _on_hierarchy_selected(self) -> None:
        item = self.hierarchy_tree.currentItem()
        self._current_treatment = None
        self._current_measurement_set = None
        if item is None:
            self.new_set_btn.setEnabled(False)
            if self._current_work is not None:
                self._render_work_detail(self._current_work)
            self._update_attach_button_visibility()
            return
        payload = item.data(0, Qt.UserRole)
        if not isinstance(payload, tuple) or len(payload) != 2:
            self._update_attach_button_visibility()
            return
        kind, entity_id = payload
        if kind == "treatment":
            treatment = TaxonTreatmentRepository.get(entity_id)
            self._current_treatment = treatment
            self.new_set_btn.setEnabled(treatment is not None)
            self.edit_selected_btn.setEnabled(treatment is not None)
            self.delete_selected_btn.setEnabled(treatment is not None)
            if treatment is not None:
                self._render_treatment_detail(treatment)
        elif kind == "measurement_set":
            ms = MeasurementSetRepository.get(entity_id)
            self._current_measurement_set = ms
            if ms is not None:
                parent_item = item.parent()
                if parent_item is not None:
                    parent_payload = parent_item.data(0, Qt.UserRole)
                    if isinstance(parent_payload, tuple) and parent_payload[0] == "treatment":
                        self._current_treatment = TaxonTreatmentRepository.get(
                            parent_payload[1]
                        )
                self._render_measurement_set_detail(ms)
                self.new_set_btn.setEnabled(self._current_treatment is not None)
                self.edit_selected_btn.setEnabled(True)
                self.delete_selected_btn.setEnabled(True)
        self._update_attach_button_visibility()

    # --- Detail rendering ---

    def _clear_hierarchy(self) -> None:
        self.hierarchy_tree.clear()
        self._current_treatment = None
        self._current_measurement_set = None
        self._add_hierarchy_placeholder(
            self.tr("Select a publication to see its treatments.")
        )

    def _clear_detail(self) -> None:
        self.detail_title.setText("")
        self.detail_view.setPlainText("")
        self.completeness_hint_label.set_hints([])
        self.edit_selected_btn.setEnabled(False)
        self.delete_selected_btn.setEnabled(False)
        self.plot_hint_label.setVisible(False)

    def _render_work_detail(self, work: ReferenceWork) -> None:
        self.detail_title.setText(work.short_label or work.title or work.id)
        self.completeness_hint_label.set_hints(
            reference_work_completeness_hints(work)
        )
        lines = [
            self.tr("Type: {type}").format(type=work.type or ""),
            self.tr("Title: {title}").format(title=work.title or ""),
        ]
        if work.year is not None:
            lines.append(self.tr("Year: {year}").format(year=work.year))
        if work.citation_key:
            lines.append(self.tr("Citation key: {value}").format(value=work.citation_key))
        if work.container_title:
            lines.append(self.tr("Container: {value}").format(value=work.container_title))
        if work.edition:
            lines.append(self.tr("Edition: {value}").format(value=work.edition))
        if work.volume:
            lines.append(self.tr("Volume: {value}").format(value=work.volume))
        if work.issue:
            lines.append(self.tr("Issue: {value}").format(value=work.issue))
        if work.pages:
            lines.append(self.tr("Pages: {value}").format(value=work.pages))
        if work.publisher:
            lines.append(self.tr("Publisher: {value}").format(value=work.publisher))
        if work.place:
            lines.append(self.tr("Place: {value}").format(value=work.place))
        if work.doi:
            lines.append(self.tr("DOI: {value}").format(value=work.doi))
        if work.isbn:
            lines.append(self.tr("ISBN: {value}").format(value=work.isbn))
        if work.url:
            lines.append(self.tr("URL: {value}").format(value=work.url))
        if work.language:
            lines.append(self.tr("Language: {value}").format(value=work.language))
        if work.citation_override:
            lines.append(self.tr("Citation override: {value}").format(value=work.citation_override))
        lines.append(self.tr("Revision: {value}").format(value=work.revision or 1))
        lines.append(self.tr("UUID: {value}").format(value=work.id))
        self.detail_view.setPlainText("\n".join(lines))
        self.plot_hint_label.setVisible(False)
        self.edit_selected_btn.setEnabled(False)
        self.delete_selected_btn.setEnabled(True)

    def _render_treatment_detail(self, treatment: TaxonTreatment) -> None:
        parent_work = self._current_work
        if parent_work is not None:
            self.detail_title.setText(
                f"{parent_work.short_label or parent_work.title or ''} — "
                f"{treatment.name_as_published or ''}"
            )
            self.completeness_hint_label.set_hints(
                reference_work_completeness_hints(parent_work)
            )
        else:
            self.detail_title.setText(treatment.name_as_published or "")
            self.completeness_hint_label.set_hints([])
        lines = [
            self.tr("Name as published: {value}").format(value=treatment.name_as_published or ""),
        ]
        if treatment.taxon_id:
            lines.append(self.tr("Taxon id: {value}").format(value=treatment.taxon_id))
        if treatment.page_from is not None or treatment.page_to is not None:
            lines.append(
                self.tr("Pages: {a}-{b}").format(
                    a=treatment.page_from if treatment.page_from is not None else "",
                    b=treatment.page_to if treatment.page_to is not None else "",
                )
            )
        if treatment.locator_text:
            lines.append(self.tr("Locator: {value}").format(value=treatment.locator_text))
        if treatment.treatment_notes:
            lines.append(self.tr("Notes: {value}").format(value=treatment.treatment_notes))
        lines.append(self.tr("Revision: {value}").format(value=treatment.revision or 1))
        lines.append(self.tr("UUID: {value}").format(value=treatment.id))
        self.detail_view.setPlainText("\n".join(lines))
        self.plot_hint_label.setVisible(False)

    def _render_measurement_set_detail(self, ms: MeasurementSet) -> None:
        work = self._current_work
        treatment = self._current_treatment
        if work is not None and treatment is not None:
            try:
                snapshot = _snapshot_measurement_details(work, treatment, ms)
            except Exception:
                snapshot = None
        else:
            snapshot = None
        self.detail_title.setText(
            self.tr("Measurement set — {kind}").format(kind=ms.data_kind or "")
        )
        if work is not None:
            self.completeness_hint_label.set_hints(
                reference_work_completeness_hints(work)
            )
        else:
            self.completeness_hint_label.set_hints([])

        lines: list[str] = []
        if snapshot is not None:
            lines.append(self.tr("Data kind: {value}").format(value=snapshot.get("data_kind") or ""))
            raw_text = snapshot.get("raw_text")
            if raw_text:
                lines.append(self.tr("Raw expression: {value}").format(value=raw_text))
            measurements = snapshot.get("measurements") or {}
            for label, key in (
                (self.tr("Length min"), "length_min"),
                (self.tr("Length core min"), "length_core_min"),
                (self.tr("Length core max"), "length_core_max"),
                (self.tr("Length max"), "length_max"),
                (self.tr("Length mean"), "length_mean"),
                (self.tr("Width min"), "width_min"),
                (self.tr("Width core min"), "width_core_min"),
                (self.tr("Width core max"), "width_core_max"),
                (self.tr("Width max"), "width_max"),
                (self.tr("Width mean"), "width_mean"),
                (self.tr("Q min"), "q_min"),
                (self.tr("Q mean"), "q_mean"),
                (self.tr("Q max"), "q_max"),
                (self.tr("Sample size"), "sample_size"),
                (self.tr("Specimen count"), "specimen_count"),
            ):
                value = measurements.get(key)
                if value is not None:
                    lines.append(f"{label}: {value}")
            raw_points = snapshot.get("raw_points")
            if isinstance(raw_points, list) and raw_points:
                lines.append(
                    self.tr("Raw points: {count} entries").format(count=len(raw_points))
                )
        else:
            lines.append(self.tr("Data kind: {value}").format(value=ms.data_kind or ""))
            if ms.raw_text:
                lines.append(self.tr("Raw expression: {value}").format(value=ms.raw_text))
        lines.append(self.tr("Revision: {value}").format(value=ms.revision or 1))
        lines.append(self.tr("UUID: {value}").format(value=ms.id))
        self.detail_view.setPlainText("\n".join(lines))

        if _measurement_set_is_plottable_hint(ms):
            self.plot_hint_label.setVisible(False)
        else:
            self.plot_hint_label.setText(
                self.tr(
                    "This measurement set is not plottable yet: it lacks a "
                    "drawable length/width rectangle, a complete mean pair, "
                    "or valid raw points."
                )
            )
            self.plot_hint_label.setVisible(True)

    # --- Actions ---

    def _on_new_work_clicked(self) -> None:
        form = _ReferenceWorkForm(self)
        if form.exec() == QDialog.Accepted and form.result_work is not None:
            self.library_changed.emit()
            self.refresh_works(select_id=form.result_work.id)

    def _on_edit_work_clicked(self) -> None:
        if self._current_work is None:
            return
        form = _ReferenceWorkForm(self, work=self._current_work)
        if form.exec() == QDialog.Accepted and form.result_work is not None:
            self.library_changed.emit()
            self.refresh_works(select_id=form.result_work.id)

    def _on_new_treatment_clicked(self) -> None:
        if self._current_work is None:
            return
        form = _TaxonTreatmentForm(
            self, reference_work_id=self._current_work.id
        )
        if form.exec() == QDialog.Accepted and form.result_treatment is not None:
            self.library_changed.emit()
            self._refresh_hierarchy_for_current_work(
                select_treatment_id=form.result_treatment.id
            )

    def _on_new_measurement_set_clicked(self) -> None:
        if self._current_treatment is None:
            return
        form = _MeasurementSetForm(
            self, taxon_treatment_id=self._current_treatment.id
        )
        if form.exec() == QDialog.Accepted and form.result_set is not None:
            self.library_changed.emit()
            self._refresh_hierarchy_for_current_work(
                select_set_id=form.result_set.id
            )

    def _on_edit_selected_clicked(self) -> None:
        if self._current_measurement_set is not None:
            form = _MeasurementSetForm(
                self,
                taxon_treatment_id=self._current_measurement_set.taxon_treatment_id,
                measurement_set=self._current_measurement_set,
            )
            if form.exec() == QDialog.Accepted and form.result_set is not None:
                self.library_changed.emit()
                self._refresh_hierarchy_for_current_work(
                    select_set_id=form.result_set.id
                )
            return
        if self._current_treatment is not None and self._current_work is not None:
            form = _TaxonTreatmentForm(
                self,
                reference_work_id=self._current_work.id,
                treatment=self._current_treatment,
            )
            if form.exec() == QDialog.Accepted and form.result_treatment is not None:
                self.library_changed.emit()
                self._refresh_hierarchy_for_current_work(
                    select_treatment_id=form.result_treatment.id
                )
            return

    def _on_delete_selected_clicked(self) -> None:
        if self._current_measurement_set is not None:
            label = self.tr("measurement set")
            delete = lambda: MeasurementSetRepository.delete(
                self._current_measurement_set.id
            )
            refresh_kind = "hierarchy"
        elif self._current_treatment is not None:
            label = self.tr("taxon treatment")
            delete = lambda: TaxonTreatmentRepository.delete(
                self._current_treatment.id
            )
            refresh_kind = "hierarchy"
        elif self._current_work is not None:
            label = self.tr("reference work")
            delete = lambda: ReferenceWorkRepository.delete(self._current_work.id)
            refresh_kind = "works"
        else:
            return

        answer = QMessageBox.question(
            self,
            self.tr("Delete library item"),
            self.tr(
                "Delete this {item}? This also deletes any unreferenced items "
                "nested beneath it."
            ).format(item=label),
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if answer != QMessageBox.Yes:
            return
        try:
            delete()
        except ReferenceLibraryError as exc:
            QMessageBox.warning(
                self,
                self.tr("Reference Library"),
                self.tr(
                    "Could not delete this library item. It may be attached to "
                    "an observation.\n\n{error}"
                ).format(error=str(exc)),
            )
            return

        self.library_changed.emit()
        if refresh_kind == "works":
            self.refresh_works()
        else:
            self._current_treatment = None
            self._current_measurement_set = None
            self._refresh_hierarchy_for_current_work()
            if self._current_work is not None:
                self._render_work_detail(self._current_work)

    def _role_display_label(self, value: str) -> str:
        return {
            "compared": self.tr("Compared"),
            "supports_identification": self.tr("Supports identification"),
            "contradicts": self.tr("Contradicts"),
        }.get(value, value)

    def _on_attach_clicked(self) -> None:
        if self._active_observation_id is None:
            return
        if self._current_measurement_set is None:
            return
        role_data = self.role_combo.currentData()
        role = (
            str(role_data)
            if isinstance(role_data, str) and role_data
            else "compared"
        )
        # The captured observation id is passed with the signal so the
        # attach handler cannot silently redirect the attachment onto a
        # different active observation should the parent's selection have
        # drifted between the manager being opened and the click.
        self.attach_requested.emit(
            self._current_measurement_set.id,
            role,
            int(self._active_observation_id),
        )
        self.accept()

    def _on_copy_curated_clicked(self) -> None:
        if self._cloud_client is None or self._sporely_taxon_id is None:
            return
        from .curated_reference_catalogue_dialog import CuratedReferenceCatalogueDialog
        dialog = CuratedReferenceCatalogueDialog(
            self, cloud_client=self._cloud_client,
            sporely_taxon_id=self._sporely_taxon_id,
        )
        dialog.copied.connect(lambda _set_id: self.refresh_works())
        dialog.exec()

    def _on_submit_curation_clicked(self) -> None:
        if self._current_measurement_set is None or self._cloud_client is None:
            return
        version = str(self._curation_attestation_version or "").strip()
        wording = str(self._curation_attestation_text or "").strip()
        if not version or not wording:
            QMessageBox.information(
                self, self.tr("Submit for curation"),
                self.tr("Reference submissions are not configured for this build."),
            )
            return
        answer = QMessageBox.question(
            self, self.tr("Submit for curation"), wording,
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No,
        )
        if answer != QMessageBox.Yes:
            return
        self.submit_curation_btn.setEnabled(False)
        thread = QThread(self)
        worker = _CurationSubmissionWorker(
            self._cloud_client, self._current_measurement_set.id, version,
        )
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.finished.connect(self._on_submission_finished)
        worker.failed.connect(self._on_submission_failed)
        worker.finished.connect(thread.quit)
        worker.failed.connect(thread.quit)
        thread.finished.connect(worker.deleteLater)
        thread.finished.connect(self._finish_pending_close)
        thread.finished.connect(thread.deleteLater)
        self._submit_thread = thread
        thread.start()

    @Slot(object)
    def _on_submission_finished(self, result: object) -> None:
        status = str(getattr(result, "status", ""))
        if not self._close_pending:
            QMessageBox.information(self, self.tr("Submit for curation"), self.tr("Submission status: {status}").format(status=status))
        self._update_attach_button_visibility()

    @Slot(str)
    def _on_submission_failed(self, message: str) -> None:
        if not self._close_pending:
            QMessageBox.warning(self, self.tr("Submit for curation"), self.tr("Could not submit reference: {error}").format(error=message))
        self._update_attach_button_visibility()

    def _finish_pending_close(self) -> None:
        self._submit_thread = None
        if self._close_pending:
            self.close()

    def closeEvent(self, event) -> None:
        if self._submit_thread is not None and self._submit_thread.isRunning():
            self._close_pending = True
            event.ignore()
            return
        super().closeEvent(event)

    def _update_attach_button_visibility(self) -> None:
        has_observation = self._active_observation_id is not None
        has_set = self._current_measurement_set is not None
        self.attach_btn.setVisible(has_observation)
        self.role_combo.setVisible(has_observation)
        self.attach_btn.setEnabled(has_observation and has_set)
        configured = bool(
            self._cloud_client is not None
            and self._curation_attestation_version
            and self._curation_attestation_text
        )
        self.submit_curation_btn.setEnabled(configured and has_set)

    # --- Public helpers used by tests / callers ---

    def current_selection_kind(self) -> str:
        if self._current_measurement_set is not None:
            return "measurement_set"
        if self._current_treatment is not None:
            return "treatment"
        if self._current_work is not None:
            return "work"
        return "empty"


__all__ = [
    "ReferenceLibraryManagerDialog",
]
