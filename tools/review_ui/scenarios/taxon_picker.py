"""Taxon picker scenarios: the unified search field, its results and the card.

The picker searches a real sample of the bundled taxonomy release
(``tools/review_ui/taxon_sample.py``), built into a throwaway database per
run. The host mimics the Species page row of the observation editor, with
room below for the result overlay.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QCheckBox, QComboBox, QHBoxLayout, QLabel, QVBoxLayout, QWidget

from ..context import ReviewContext
from ..registry import ReviewScenario, ScenarioRegistry
from ..taxon_sample import sample_lookup

_LABEL_WIDTH = 96


def _dialog_tr(text: str) -> str:
    """The observation editor's own label, translated as it is there."""
    from PySide6.QtCore import QCoreApplication

    return QCoreApplication.translate("ObservationDetailsDialog", text)


def _lookup(context: ReviewContext):
    directory = Path(context.enter_fixture(tempfile.TemporaryDirectory()))
    return sample_lookup(directory / "taxonomy.sqlite3")


def _picker(context: ReviewContext, *, note: bool = False):
    from ui.taxon_picker import TaxonPicker

    settings = {} if note else {"taxon_picker_source_note_dismissed": "1"}
    return TaxonPicker(
        _lookup(context),
        debounce_ms=0,
        setting_get=lambda key, default: settings.get(key, default),
        setting_set=settings.__setitem__,
    )


def _species_page(picker, *, extra_rows: bool = True) -> QWidget:
    """The picker in an observation-editor-like Species row."""
    host = QWidget()
    host.setObjectName("taxonomyPage")
    layout = QVBoxLayout(host)
    layout.setContentsMargins(10, 10, 10, 10)
    layout.setSpacing(8)
    row = QHBoxLayout()
    label = QLabel(_dialog_tr("Species:"))
    label.setFixedWidth(_LABEL_WIDTH)
    row.addWidget(label, 0, Qt.AlignTop)
    row.addWidget(picker, 1)
    layout.addLayout(row)
    if extra_rows:
        for text in ("Uncertain", "Alien or cultivated"):
            line = QHBoxLayout()
            line.addSpacing(_LABEL_WIDTH + 6)
            line.addWidget(QCheckBox(_dialog_tr(text)))
            line.addStretch(1)
            layout.addLayout(line)
        method = QHBoxLayout()
        method_label = QLabel(_dialog_tr("Determination:"))
        method_label.setFixedWidth(_LABEL_WIDTH)
        method.addWidget(method_label)
        combo = QComboBox()
        combo.addItems(["", _dialog_tr("Microscopy")])
        method.addWidget(combo, 1)
        layout.addLayout(method)
    layout.addStretch(1)
    host._picker = picker  # the capture hooks reach it here
    return host


def _search(query: str, *, choice_name: str | None = None, choice: int = 0, scroll_end: bool = False):
    def capture(host):
        from PySide6.QtWidgets import QApplication

        picker = host._picker
        picker.search_for(query)
        if choice_name is not None:
            row = next(i for i, r in enumerate(picker.results()) if r.name == choice_name and r.has_source_choice)
            picker.popup.view.setCurrentIndex(picker.model.index(row, 0))
            picker._list_mode = True
            for _ in range(choice):
                picker._set_choice(1)
        if scroll_end:
            last = picker.model.rowCount() - 2  # "Show more"; the free-name row follows
            picker.popup.view.setCurrentIndex(picker.model.index(last, 0))
            picker.popup.view.scrollToBottom()
        QApplication.processEvents()
        return host
    return capture


def _build_search(note: bool = False, evidence: tuple[int, ...] = (), ai: bool = False):
    def build(context: ReviewContext):
        picker = _picker(context, note=note)
        picker.set_evidence_taxon_ids(evidence)
        if ai:
            _add_ai(picker)
        return _species_page(picker)
    return build


def _add_ai(picker) -> None:
    from ui.taxon_picker import AiSuggestion

    # Artsorakel results as the observation editor holds them: NBIC:54701
    # resolves to the NorTaxa entry 625621; the third has no identifier.
    picker.set_ai_suggestions([
        AiSuggestion("vintersopp", "Flammulina velutipes", 0.82, 625621),
        AiSuggestion("langsporet vintersopp", "Flammulina elastica", 0.09, 98272),
        AiSuggestion("Gymnopus", "Gymnopus", 0.03, None),
    ])


def _pick(picker, query: str, name: str, source: str | None = None, kind: str = "taxon"):
    page = picker._lookup.search_taxa(query, limit=200, evidence_taxon_ids=picker._evidence_ids)
    for row in page.rows:
        if row.name == name and row.kind == kind:
            concept = next(c for c in row.concepts if source is None or c.source_name == source)
            picker.show_selection(row, concept)
            return row, concept
    raise LookupError(f"{name} not found for {query!r}")


def _build_card(context: ReviewContext):
    picker = _picker(context)
    _add_ai(picker)
    picker.set_evidence_taxon_ids([625621])
    _pick(picker, "Flammulina velutipes", "Flammulina velutipes", "NorTaxa")
    return _species_page(picker)


def _card_states(context: ReviewContext):
    host = QWidget()
    host.setObjectName("taxonomyPage")
    layout = QVBoxLayout(host)
    layout.setContentsMargins(10, 10, 10, 10)
    layout.setSpacing(4)
    lookup = _lookup(context)

    def add(title: str, setup) -> None:
        picker = _picker(context)
        picker.set_lookup(lookup)
        setup(picker)
        layout.addWidget(QLabel(f"<b>{title}</b>"))
        layout.addWidget(picker)

    add("Same name in two sources, families differ", lambda p: _pick(p, "Cantharellus", "Cantharellus", "NorTaxa"))
    add("Synonym", lambda p: _pick(p, "Collybia velutipes", "Collybia velutipes", kind="synonym"))
    add("Genus only, uncertain", lambda p: (_pick(p, "Russula", "Russula"), p.set_uncertain(True)))
    add("Species group", lambda p: _pick(p, "Russula queletii", "Russula queletii coll."))
    add("Same-source homonym", lambda p: _pick(p, "Aschersonia", "Aschersonia"))
    add("Name without a taxon", lambda p: p.show_unlinked_name("Cortinarius sp. «SKA-2026-14»"))
    add("Older observation, not linked", lambda p: p.show_unlinked_name("Russula aeruginea", legacy=True))
    layout.addStretch(1)
    return host


def register_taxon_picker_scenarios(registry: ScenarioRegistry) -> None:
    for scenario in (
        ReviewScenario(
            id="taxon-picker.search-genus",
            group="taxon-picker",
            title="Taxon picker — genus search with the first-time note",
            description=(
                "Flammulina: the genus row offers the NorTaxa and COL entries, NorTaxa selected; "
                "the first-time note explains the two sources."
            ),
            viewport=(640, 600),
            locale="nb_NO",
            build=_build_search(note=True),
            capture_target=_search("Flammulina"),
        ),
        ReviewScenario(
            id="taxon-picker.search-epithet-switched",
            group="taxon-picker",
            title="Taxon picker — epithet search, source switched with the keyboard",
            description=(
                "velutipes across genera, with the synonym Collybia velutipes; the Flammulina velutipes row "
                "is switched to COL before anything is picked."
            ),
            viewport=(640, 600),
            locale="nb_NO",
            build=_build_search(),
            capture_target=_search("velutipes", choice_name="Flammulina velutipes", choice=1),
        ),
        ReviewScenario(
            id="taxon-picker.search-evidence",
            group="taxon-picker",
            title="Taxon picker — Artsorakel evidence sets the default source",
            description=(
                "The Artsorakel identifier resolves to the NorTaxa entry: its row comes first and the NorTaxa "
                "chip is the default; the COL entry is still offered."
            ),
            viewport=(640, 600),
            locale="nb_NO",
            build=_build_search(evidence=(625621,), ai=True),
            capture_target=_search("vintersopp"),
        ),
        ReviewScenario(
            id="taxon-picker.search-group-show-more",
            group="taxon-picker",
            title="Taxon picker — Norwegian group name, end of the first page",
            description="kremler: Russula first, then species whose names contain kreml; Show more at the end.",
            viewport=(640, 600),
            locale="nb_NO",
            build=_build_search(),
            capture_target=_search("kremler", scroll_end=True),
        ),
        ReviewScenario(
            id="taxon-picker.search-family-split",
            group="taxon-picker",
            title="Taxon picker — same name, different families per source",
            description="Cantharellus: NorTaxa places it in Cantharellaceae and COL in Hydnaceae; each family is shown with its source.",
            viewport=(640, 600),
            locale="nb_NO",
            build=_build_search(),
            capture_target=_search("Cantharellus"),
        ),
        ReviewScenario(
            id="taxon-picker.card",
            group="taxon-picker",
            title="Taxon picker — identification card with Artsorakel suggestions",
            description="Flammulina velutipes picked from NorTaxa; the source can be switched on the card.",
            viewport=(640, 260),
            locale="nb_NO",
            build=_build_card,
        ),
        ReviewScenario(
            id="taxon-picker.card-states",
            group="taxon-picker",
            title="Taxon picker — card states",
            description="Family split, synonym, genus only with Uncertain, group, homonym, free name and an unlinked older name.",
            viewport=(640, 860),
            locale="nb_NO",
            build=_card_states,
        ),
        ReviewScenario(
            id="taxon-picker.search-genus-dark",
            group="taxon-picker",
            title="Taxon picker — genus search, dark theme",
            description="The Flammulina search in the dark theme.",
            viewport=(640, 600),
            theme="dark",
            locale="nb_NO",
            build=_build_search(note=True),
            capture_target=_search("Flammulina"),
        ),
        ReviewScenario(
            id="taxon-picker.card-states-dark",
            group="taxon-picker",
            title="Taxon picker — card states, dark theme",
            description="The card states in the dark theme.",
            viewport=(640, 860),
            theme="dark",
            locale="nb_NO",
            build=_card_states,
        ),
    ):
        registry.register(scenario)
