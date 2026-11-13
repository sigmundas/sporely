"""Tabbed "Add reference" picker dialog.

Consolidates the reference-add entry points into one dialog: source tabs
(Library / Community / My observations / Enter manually) sharing a single
:class:`ReferencePreviewPane`, docked beside the tab widget in a top-level
splitter so every tab's selection populates the same preview instance
instead of each tab owning its own. All four tabs are wired.

The Community tab embeds :class:`~ui.cloud_reference_dialog.CommunityResultsPane`,
which relocates ``CloudReferenceDialog``'s browse/select flow (its search
workers and payload builders, reused rather than duplicated) into the
picker. Unlike that legacy dialog, genus/species are fixed by the picker's
working taxon, so results load automatically instead of behind a search
button, and the dialog's two footer buttons ("Import summary as reference" /
"Use raw points for plot") become a radio the tab exposes instead
("Range summary" / "Raw points (n=X)"). A cloud entry has no measurement-set
or observation identity, so "Add to plot" routes through a dedicated
``cloud_attach_callback(dict)`` rather than the string-identifier
``attach_callback`` the other tabs use, straight to the same
``_add_reference_series_entry`` path the legacy dialog already calls
directly -- no new persistence. ``CloudReferenceDialog`` itself stays
reachable for now; stage 6 removes that dead entry point.

The Library tab does not fork any add/attach logic: selecting a result and
clicking "Add to plot" calls the ``attach_callback`` supplied by the host
(MainWindow), which is expected to be
``MainWindow._attach_normalized_reference_to_active_observation`` — the
exact path the (soon to be retired) ``ReferenceLibraryAttachDialog`` uses.
Color assignment is not touched here: it happens automatically, keyed by
list position, inside ``MainWindow._resolved_reference_series_entries``.

Its list carries two states that must not be collapsed into one. The
*selected* row is what the shared preview shows; the *checked* rows are what
the footer will add to the plot. Rows are grouped by relevance to the picker's
taxon (``group_library_candidates``) and rendered by
:class:`~ui.library_source_row.LibrarySourceRow`. Each checked row carries its
own role selector (Compared / Supports identification / Contradicts, default
Compared) -- the role of the *use* that row would create, not of the
publication -- and the footer offers a "Set all" shortcut once two or more
rows are queued. Adding several sources routes through
``library_attach_callback(measurement_set_id, role) -> (status, reason)``,
called once per checked row in the order the rows were checked. ``status`` is
``"attached"``, ``"already_attached"``, ``"failed"`` or
``"observation_changed"`` (a failure a retry here cannot fix); succeeded items stay
attached even when a later one fails (no rollback), the dialog lists every
item's outcome, unchecks the ones that landed and keeps the failed ones
checked so the user can retry them, and closes only when nothing failed.

The My observations tab lists previous observations of the working taxon —
the same query (``ObservationDB.get_personal_observations_for_species``)
that populates the legacy Source dropdown's "My data <date>" entries — and
routes "Add to plot" through the same ``attach_callback``, but with an
``"observation:<id>"``-prefixed identifier: a personal observation has no
normalized measurement-set identity, so the host dispatches that prefix to
the legacy ``source_kind == "observation"`` comparison-series path instead
of the measurement-set attach path.

The Enter-manually tab embeds a
:class:`~ui.reference_entry_editor.ReferenceEntryEditor` — the same paste/
parse table, publication picker, and Data section the legacy Quick-add
dialog uses, without its modal Save/Cancel chrome — sharing the shared
:class:`ReferencePreviewPane` and this dialog's "Add to plot"/Cancel footer
instead. "Add to plot" routes through a dedicated
``manual_attach_callback(editor) -> bool`` rather than the string-identifier
``attach_callback``, since a manual submission can fail validation or an
observation-drift check *after* the click and must leave the picker open
rather than claim success — the callback's return value tells this dialog
whether to close. Changing the "Compare against" target rebinds the
editor's publication/treatment identity (see
:meth:`ReferenceEntryEditor.set_comparison_target`) without discarding
already-entered measurement values, and to the submission path,
``ReferenceEntryEditor`` is duck-typed identically to the legacy dialog's
own wrapper, so ``MainWindow`` reuses one shared post-validation
persistence routine for both.

That tab also owns the footer's second action, "Save to library"
(``manual_save_callback(editor) -> str | None``), which is hidden on the
other three tabs — their sources are already stored somewhere. Saving is
not plotting: it writes the library hierarchy, leaves the picker open and
the plot untouched, and returns the new measurement set's id. While that id
is held, "Add to plot" attaches *that* set through
``attach_saved_set_callback(measurement_set_id) -> bool`` rather than
submitting the editor a second time, which would store the same typed data
twice. Editing the form afterwards drops the id, because the saved set no
longer describes what is on screen.

``confirm_attach_callback(roles) -> bool`` is asked once, with the role of
every queued Library source, before a Library add attaches anything; the
host uses it for the notice shown when the observation is already public.
False attaches nothing and leaves the selection as it was.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from PySide6.QtCore import QCoreApplication, QSize
from PySide6.QtGui import QPalette
from PySide6.QtWidgets import (
    QScrollArea,
    QStyle,
)

from database.models import MeasurementDB, ObservationDB
from database.taxon_suggestions import (
    KIND_VERIFIED,
)
from references.library_taxon_scope import matches_library_taxon

from database.reference_library import (
    MeasurementSetCandidate,
)


from . import measurement_content_view as mcv

_USABLE_MEASUREMENT_TYPES = (None, "", "manual", "spore", "spores")


def filter_library_candidates(
    candidates: Iterable[MeasurementSetCandidate],
    *,
    taxon_id: str | None,
    only_this_taxon: bool,
    query: str = "",
    taxon_text: str = "",
) -> list[MeasurementSetCandidate]:
    """Filter by concept ID or complete published-name relevance, then search.

    A name match helps discovery when historical treatments have no ID or
    another COL/NorTaxa concept. It never changes the stored taxon identity.
    """
    result = list(candidates)
    if only_this_taxon:
        result = [
            c for c in result
            if matches_library_taxon(c, taxon_id=taxon_id, taxon_name=taxon_text)
        ]
    normalized_query = (query or "").strip().casefold()
    if normalized_query:
        def _match(c: MeasurementSetCandidate) -> bool:
            for field_value in (c.short_label, c.name_as_published, c.raw_text):
                if field_value and normalized_query in str(field_value).casefold():
                    return True
            return False

        result = [c for c in result if _match(c)]
    return result


#: Library relevance groups, in the fixed display order of the design
#: contract (N6). The strings are keys, not wording: the headings are
#: translated in :func:`library_group_heading`.
LIBRARY_GROUP_ORDER = ("this_taxon", "same_genus", "rest")


def _published_genus(candidate: MeasurementSetCandidate) -> str:
    """First word of a candidate's published name, which is its genus.

    Published names carry authorities and infraspecific ranks
    (``Cortinarius limonius (Fr.) Fr.``), so nothing but the leading word is
    reliable here — and nothing more than the genus is needed.
    """
    name = str(candidate.name_as_published or "").strip()
    return name.split()[0] if name else ""


def library_relevance(
    candidate: MeasurementSetCandidate,
    *,
    taxon_id: str | None,
    genus: str,
    taxon_name: str,
) -> str:
    """Which relevance group one candidate belongs to.

    Two independent signals establish "this taxon", because the picker's
    target does not always have an id: an AI suggestion or a freely-typed
    binomial gives a name and nothing else (see
    ``AddReferenceDialog._taxon_target_query_text``). Matching on either the
    id *or* the published name keeps a hand-typed target grouping the same
    rows an identified one would, rather than silently demoting every match
    to ``same_genus``.
    """
    if matches_library_taxon(candidate, taxon_id=taxon_id, taxon_name=taxon_name):
        return "this_taxon"
    target_genus = genus.strip().casefold()
    if target_genus and _published_genus(candidate).casefold() == target_genus:
        return "same_genus"
    return "rest"


def group_library_candidates(
    candidates: Iterable[MeasurementSetCandidate],
    *,
    taxon_id: str | None,
    genus: str,
    taxon_name: str,
) -> list[tuple[str, list[MeasurementSetCandidate]]]:
    """Split candidates into the contract's three groups, in fixed order.

    Pure, and deliberately separate from :func:`filter_library_candidates`:
    filtering decides what the user may see at all, grouping only decides the
    order it is shown in. Empty groups are dropped rather than rendered as a
    heading with ``(0)``, so the list never advertises a category the library
    cannot fill. Within a group the caller's order is preserved.
    """
    buckets: dict[str, list[MeasurementSetCandidate]] = {
        key: [] for key in LIBRARY_GROUP_ORDER
    }
    for candidate in candidates:
        buckets[
            library_relevance(
                candidate, taxon_id=taxon_id, genus=genus, taxon_name=taxon_name
            )
        ].append(candidate)
    return [(key, buckets[key]) for key in LIBRARY_GROUP_ORDER if buckets[key]]


def library_group_heading(group: str, count: int) -> str:
    """The heading of a relevance group in the library list (the reference drawer)."""
    if group == "this_taxon":
        template = QCoreApplication.translate("AddReferenceDialog", "This taxon ({count})")
    elif group == "same_genus":
        template = QCoreApplication.translate("AddReferenceDialog", "Same genus ({count})")
    else:
        template = QCoreApplication.translate("AddReferenceDialog", "Rest of library ({count})")
    return template.format(count=count)


def relevance_badge_text(group: str) -> str:
    """Badge wording for a relevance group, or ``""`` for no badge.

    "Rest of library" gets none: a badge there would label the absence
    of a relationship, which is what the grouping already shows.
    """
    if group == "this_taxon":
        return QCoreApplication.translate("AddReferenceDialog", "Same taxon")
    if group == "same_genus":
        return QCoreApplication.translate("AddReferenceDialog", "Same genus")
    return ""


def semantic_badge_text(candidate: MeasurementSetCandidate) -> str:
    """The data-semantics badge, straight from the Stage 1 projection.

    Which badge a row deserves was decided by
    :attr:`SourceDisplay.data_label`, so no widget re-reads ``data_kind``
    or a database column to guess (design contract N10–N14). The wording
    is :func:`ui.measurement_content_view.data_label_text`, shared with the
    manual editor's preview.
    """
    return mcv.data_label_text(candidate.source_display().data_label)


@dataclass
class PersonalObservationCandidate:
    """One row in the My observations tab: a different personal observation
    of the working taxon, with spore measurements usable as a comparison
    series."""

    observation_id: int
    date: str
    author: str
    location: str
    points: list[dict]
    # The row's own taxon. Only meaningful when the tab is browsing a whole
    # genus, where several species share the list and the date/author label
    # alone does not say which one a row is.
    genus: str = ""
    species: str = ""

    @property
    def n(self) -> int:
        return len(self.points)


def default_my_observation_candidates(
    genus: str, species: str = "", *, exclude_observation_id: int | None = None
) -> list[PersonalObservationCandidate]:
    """Load My-observations candidates from the same query that populates
    the legacy Source dropdown's "My data <date>" entries.

    Only observations with at least one usable spore measurement are
    returned (mirrors the point-filtering in
    ``MainWindow._maybe_load_reference_panel_reference``'s observation
    branch), since an entry with no points cannot be plotted.

    A genus with no species browses every personal observation in that
    genus. This tab previously required both and returned nothing for a
    genus-only identification, which is the common case while an
    observation is still being worked out -- exactly when comparing it
    against one's own earlier collections is most useful.
    """
    if not genus:
        return []
    rows = ObservationDB.get_personal_observations_for_species(
        genus, species, exclude_observation_id=exclude_observation_id
    )
    result: list[PersonalObservationCandidate] = []
    for row in rows:
        try:
            obs_id = int(row["id"])
        except (KeyError, TypeError, ValueError):
            continue
        raw = MeasurementDB.get_measurements_for_observation(obs_id)
        points = [
            m for m in raw
            if m.get("length_um") is not None
            and m.get("width_um") is not None
            and m.get("measurement_type") in _USABLE_MEASUREMENT_TYPES
        ]
        if not points:
            continue
        obs = ObservationDB.get_observation(obs_id) or {}
        date_str = (row.get("date") or "").split(" ")[0].split("T")[0]
        result.append(
            PersonalObservationCandidate(
                observation_id=obs_id,
                date=date_str,
                author=(row.get("author") or "").strip(),
                location=(obs.get("location") or "").strip(),
                points=points,
                genus=str(row.get("genus") or "").strip(),
                species=str(row.get("species") or "").strip(),
            )
        )
    return result


class EditorScrollArea(QScrollArea):
    """A :class:`QScrollArea` that reports its hosted widget's own size hint.

    ``QScrollArea.sizeHint()`` is a fixed style heuristic that ignores how
    large the hosted widget actually wants to be. Hosting the manual editor
    in a plain scroll area therefore hid the editor's size from the picker's
    derived default size, which opened ~250px too short and put scrollbars
    on the editor at its *default* size -- not just when deliberately
    shrunk.

    Only ``sizeHint`` is overridden, never ``minimumSizeHint``: the whole
    point of the scroll area is that the dialog can still be resized far
    below this hint, at which point the scrollbars are the correct
    fallback.
    """

    def sizeHint(self) -> QSize:  # noqa: N802 - Qt override
        widget = self.widget()
        if widget is None:
            return super().sizeHint()
        hint = widget.sizeHint()
        # Reserve one scrollbar extent on each axis so the hint describes a
        # size at which the editor fits with the scrollbars absent, rather
        # than one where showing a scrollbar immediately clips the content.
        extent = self.style().pixelMetric(QStyle.PM_ScrollBarExtent)
        margins = self.contentsMargins()
        frame = 2 * self.frameWidth()
        return QSize(
            hint.width() + extent + margins.left() + margins.right() + frame,
            hint.height() + extent + margins.top() + margins.bottom() + frame,
        )


__all__ = [
    "LIBRARY_GROUP_ORDER",
    "filter_library_candidates",
    "group_library_candidates",
    "library_group_heading",
    "library_relevance",
    "relevance_badge_text",
    "semantic_badge_text",
    "PersonalObservationCandidate",
    "default_my_observation_candidates",
]
