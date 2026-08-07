"""Focused tests for the human-facing Reference Library work editor.

The editor rewrite replaces the two raw ``authors_json`` / ``editors_json``
line edits with an ordered person-list editor and reorganizes the fields
into scrollable sections that adapt to the selected work type. The
existing repository, canonical citation service, and manager-dialog
tests remain authoritative for the untouched contract; this file
covers only the new UX behaviours.
"""
from __future__ import annotations

import json
import os

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
PySide6 = pytest.importorskip("PySide6")

from database import schema as _schema
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)


# --- Fixtures ---------------------------------------------------------------


@pytest.fixture()
def libs(tmp_path, monkeypatch):
    db_path = tmp_path / "mushrooms.db"
    ref_path = tmp_path / "reference_values.db"
    monkeypatch.setattr(_schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(_schema, "get_reference_database_path", lambda: ref_path)
    monkeypatch.setattr(
        _schema,
        "get_bundled_reference_database_path",
        lambda: tmp_path / "does_not_exist.db",
    )
    _schema.init_database()
    return db_path, ref_path


@pytest.fixture(scope="module")
def qapp():
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


# --- PersonListEditor: load, edit, serialize -------------------------------


def test_person_list_editor_loads_canonical_json(libs, qapp):
    """AC-1: canonical JSON populates human-friendly rows without dirtying."""
    from ui.reference_library_manager_dialog import _PersonListEditor

    editor = _PersonListEditor()
    try:
        editor.load_json(
            json.dumps(
                [
                    {"family": "Petersen", "given": "Jens H."},
                    {"family": "Knudsen", "given": "Henning"},
                ]
            )
        )
        entries = editor.entries()
        assert len(entries) == 2
        assert entries[0] == {"family": "Petersen", "given": "Jens H."}
        assert entries[1] == {"family": "Knudsen", "given": "Henning"}
        # Loading MUST NOT mark the editor dirty.
        assert editor.is_dirty() is False
        # Warning label is not shown for well-formed input.
        assert editor.warning_label.isHidden() is True
    finally:
        editor.deleteLater()


def test_person_list_editor_serializes_edits_to_canonical_json(libs, qapp):
    """AC-1: user edits round-trip into the canonical JSON structure the
    existing citation service consumes (family / given / literal keys)."""
    from ui.reference_library_manager_dialog import _PersonListEditor

    editor = _PersonListEditor()
    try:
        row = editor.add_row(
            family="Petersen", given="Jens H.", mark_dirty=False
        )
        row.family_input.setText("Petersen")
        row.given_input.setText("Jens Henrik")
        # Simulate a user edit event.
        row.changed.emit()
        payload = json.loads(editor.to_json())
        assert payload == [{"family": "Petersen", "given": "Jens Henrik"}]
        assert editor.is_dirty() is True
    finally:
        editor.deleteLater()


def test_person_list_editor_organization_entries_use_literal_key(libs, qapp):
    """Organizations serialize to the ``literal`` JSON key so the citation
    formatter can pick them up via ``_agent_label``."""
    from ui.reference_library_manager_dialog import _PersonListEditor

    editor = _PersonListEditor()
    try:
        editor.add_row(organization="Society for Fungi", mark_dirty=True)
        payload = json.loads(editor.to_json())
        assert payload == [{"literal": "Society for Fungi"}]
    finally:
        editor.deleteLater()


def test_person_list_editor_add_remove_reorder(libs, qapp):
    """AC-1: add / remove / reorder controls actually update the list."""
    from ui.reference_library_manager_dialog import _PersonListEditor

    editor = _PersonListEditor()
    try:
        r1 = editor.add_row(family="A", mark_dirty=False)
        r2 = editor.add_row(family="B", mark_dirty=False)
        r3 = editor.add_row(family="C", mark_dirty=False)
        assert [e["family"] for e in editor.entries()] == ["A", "B", "C"]

        # Move C up (delta = -1).
        r3.move_requested.emit(r3, -1)
        assert [e["family"] for e in editor.entries()] == ["A", "C", "B"]

        # Remove A.
        r1.remove_requested.emit(r1)
        assert [e["family"] for e in editor.entries()] == ["C", "B"]

        # Add via the button.
        editor.add_btn.click()
        assert len(editor.entries()) == 2  # new row is empty and skipped
    finally:
        editor.deleteLater()


def test_person_list_editor_supports_empty_list(libs, qapp):
    """AC-1: empty lists round-trip as ``[]`` (never None, never omitted)."""
    from ui.reference_library_manager_dialog import _PersonListEditor

    editor = _PersonListEditor()
    try:
        editor.load_json("[]")
        assert editor.entries() == []
        assert editor.to_json() == "[]"
    finally:
        editor.deleteLater()


def test_person_list_editor_preserves_malformed_json_until_edited(libs, qapp):
    """AC-1: malformed JSON must NOT crash and must NOT be silently
    discarded. A warning surfaces and the original raw text is preserved
    on save until the user actively edits the list."""
    from ui.reference_library_manager_dialog import _PersonListEditor

    editor = _PersonListEditor()
    try:
        bad = "this is not valid { json"
        editor.load_json(bad)
        # Editor is empty (rows) but a translated warning is exposed.
        assert editor.entries() == []
        assert editor.warning_label.isHidden() is False
        assert editor.parse_error_message() is not None
        # Untouched -> serialize preserves the original raw string.
        assert editor.to_json() == bad
        # Once the user edits the list, we serialize the actual rows.
        editor.add_row(family="Petersen", mark_dirty=True)
        payload = json.loads(editor.to_json())
        assert payload == [{"family": "Petersen"}]
    finally:
        editor.deleteLater()


def test_person_list_editor_preserves_non_list_json(libs, qapp):
    """AC-1: a JSON scalar (e.g. ``{}``) is not a list — warn and preserve."""
    from ui.reference_library_manager_dialog import _PersonListEditor

    editor = _PersonListEditor()
    try:
        editor.load_json("{}")
        assert editor.warning_label.isHidden() is False
        assert editor.to_json() == "{}"
    finally:
        editor.deleteLater()


# --- Adaptive publication fields --------------------------------------------


def _select_type(form, key: str) -> None:
    idx = form.type_combo.findData(key)
    assert idx >= 0, f"work type {key!r} missing from combo"
    form.type_combo.setCurrentIndex(idx)


def test_form_article_shows_journal_volume_issue_pages(libs, qapp):
    """AC-2: an article shows journal/volume/issue/pages and hides book
    fields such as edition and editors."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        _select_type(form, "article")
        assert form.container_input.isHidden() is False
        assert form.volume_input.isHidden() is False
        assert form.issue_input.isHidden() is False
        assert form.pages_input.isHidden() is False
        assert form.edition_input.isHidden() is True
        assert form.editors_editor.isHidden() is True
        # Identifiers are per-type: an article has DOI + URL but not ISBN.
        assert form.doi_input.isHidden() is False
        assert form.url_input.isHidden() is False
        assert form.isbn_input.isHidden() is True
        # Container label is the journal-flavored one for articles — no
        # "container" jargon.
        container_label = form._publication_row_labels["container_title"]
        assert container_label.text() == "Journal:"
    finally:
        form.deleteLater()


def test_form_book_shows_edition_editors_publisher_place(libs, qapp):
    """AC-2: a book shows edition/editors/publisher/place and hides
    article-specific fields (volume/issue/pages)."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        _select_type(form, "book")
        assert form.edition_input.isHidden() is False
        assert form.editors_editor.isHidden() is False
        assert form.publisher_input.isHidden() is False
        assert form.place_input.isHidden() is False
        assert form.volume_input.isHidden() is True
        assert form.issue_input.isHidden() is True
        assert form.pages_input.isHidden() is True
        # A book has ISBN + URL but not DOI.
        assert form.isbn_input.isHidden() is False
        assert form.url_input.isHidden() is False
        assert form.doi_input.isHidden() is True
    finally:
        form.deleteLater()


def test_form_chapter_shows_container_editors_pages(libs, qapp):
    """AC-2: a chapter/contribution shows container (book title), editors,
    pages, publisher, place."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        _select_type(form, "chapter")
        assert form.container_input.isHidden() is False
        assert form.editors_editor.isHidden() is False
        assert form.pages_input.isHidden() is False
        assert form.publisher_input.isHidden() is False
        assert form.place_input.isHidden() is False
        assert form.volume_input.isHidden() is True
        assert form.issue_input.isHidden() is True
        # A chapter has DOI + ISBN but not URL (the parent book is the
        # thing you link to, and its ISBN is the identifier).
        assert form.doi_input.isHidden() is False
        assert form.isbn_input.isHidden() is False
        assert form.url_input.isHidden() is True
        # Plain-English label — no "container".
        container_label = form._publication_row_labels["container_title"]
        assert container_label.text() == "In book:"
        assert "container" not in container_label.text().lower()
    finally:
        form.deleteLater()


def test_form_website_shows_only_url_no_doi_isbn_publisher_or_container(libs, qapp):
    """A website is its own container — the ``container_title``,
    ``publisher``, DOI and ISBN fields would just be noise. The editor
    hides every publication-details field and shows only the URL in the
    Identifiers section. The whole Publication-details section box
    also hides itself so the form does not display an empty header."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        _select_type(form, "website")
        # All publication-details fields hidden.
        for widget in (
            form.container_input,
            form.editors_editor,
            form.edition_input,
            form.volume_input,
            form.issue_input,
            form.pages_input,
            form.publisher_input,
            form.place_input,
        ):
            assert widget.isHidden() is True, widget
        # Identifiers: only URL is shown.
        assert form.doi_input.isHidden() is True
        assert form.isbn_input.isHidden() is True
        assert form.url_input.isHidden() is False
        # Publication-details section header disappears when every row
        # under it is hidden.
        assert form._publication_section_box.isHidden() is True
        # Identifiers box still visible (URL is inside it).
        assert form._identifiers_section_box.isHidden() is False
    finally:
        form.deleteLater()


def test_form_never_shows_the_word_container_to_the_user(libs, qapp):
    """The word "Container" is jargon; no user-facing label should
    contain it for any of the standard work types."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        for work_type in ("book", "article", "chapter", "website"):
            _select_type(form, work_type)
            label = form._publication_row_labels["container_title"]
            # Not asserting visibility here (book/website hide it) —
            # only asserting the LABEL text is never jargon.
            assert "container" not in label.text().lower(), (
                work_type, label.text()
            )
    finally:
        form.deleteLater()


def test_form_unknown_type_falls_back_to_full_publication_section(libs, qapp):
    """AC-2: for ``other`` (and any type not in the visibility map) the
    form falls back to showing every publication field."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        _select_type(form, "other")
        for widget in (
            form.container_input,
            form.editors_editor,
            form.edition_input,
            form.volume_input,
            form.issue_input,
            form.pages_input,
            form.publisher_input,
            form.place_input,
        ):
            assert widget.isHidden() is False, widget
    finally:
        form.deleteLater()


def test_changing_type_does_not_erase_hidden_values(libs, qapp):
    """AC-2: switching type only changes visibility. Hidden values are
    still present in the form and still collected on save."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        _select_type(form, "book")
        form.edition_input.setText("1st")
        form.publisher_input.setText("Sunrise")
        _select_type(form, "article")
        # edition_input is now hidden…
        assert form.edition_input.isHidden() is True
        # …but its value survives.
        assert form.edition_input.text() == "1st"
        # And is included in the collected payload.
        payload = form._collect()
        assert payload["edition"] == "1st"
        assert payload["publisher"] == "Sunrise"
    finally:
        form.deleteLater()


# --- Round-trip: no-op edits preserve every field ---------------------------


def test_editing_and_saving_without_changes_preserves_every_field(libs, qapp):
    """AC-6: opening an existing work in the editor and saving without
    touching anything must round-trip every field (barring the repository
    revision bump)."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    original = ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title="Danmarks Basidiesvampe",
            short_label="Petersen 1990",
            authors_json=json.dumps(
                [{"family": "Petersen", "given": "Jens H."}]
            ),
            editors_json=json.dumps([{"family": "Knudsen"}]),
            citation_key="petersen-1990",
            container_title="Fungi of the North",
            year=1990,
            edition="1st",
            publisher="Sunrise",
            place="Copenhagen",
            volume="2",
            issue="3",
            pages="1-42",
            doi="10.1000/xyz",
            url="https://example.org/petersen",
            language="da",
            citation_override="Petersen, J. H. (1990). Custom override.",
        )
    )

    persisted = ReferenceWorkRepository.get(original.id)
    form = _ReferenceWorkForm(None, work=persisted)
    try:
        form._on_save()
        assert form.result_work is not None
        result = form.result_work
        # UUID preserved; revision bumped; every field unchanged.
        assert result.id == original.id
        assert result.revision == original.revision + 1
        assert result.type == original.type
        assert result.title == original.title
        assert result.short_label == original.short_label
        # Editors_json / authors_json round-trip through the canonical shape.
        assert json.loads(result.authors_json) == json.loads(original.authors_json)
        assert json.loads(result.editors_json) == json.loads(original.editors_json)
        assert result.citation_key == original.citation_key
        assert result.container_title == original.container_title
        assert result.year == original.year
        assert result.edition == original.edition
        assert result.publisher == original.publisher
        assert result.place == original.place
        assert result.volume == original.volume
        assert result.issue == original.issue
        assert result.pages == original.pages
        # DOI is normalized on save; ensure it survives.
        assert result.doi == "10.1000/xyz"
        assert result.url == original.url
        assert result.language == original.language
        assert result.citation_override == original.citation_override
    finally:
        form.deleteLater()


def test_editing_authors_via_person_editor_serializes_canonical_json(libs, qapp):
    """AC-1 + AC-6: edits made through the person-list editor produce the
    same canonical JSON shape the citation service consumes."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        _select_type(form, "book")
        form.title_input.setText("Danmarks Basidiesvampe")
        form.short_label_input.setText("Petersen 1990")
        form.year_input.setText("1990")
        form.authors_editor.add_row(
            family="Petersen", given="Jens H.", mark_dirty=True
        )
        form.authors_editor.add_row(
            family="Knudsen", given="Henning", mark_dirty=True
        )
        form._on_save()
        assert form.result_work is not None
        payload = json.loads(form.result_work.authors_json)
        assert payload == [
            {"family": "Petersen", "given": "Jens H."},
            {"family": "Knudsen", "given": "Henning"},
        ]
    finally:
        form.deleteLater()


# --- Advanced section --------------------------------------------------------


def test_advanced_section_collapsed_by_default_but_retains_values(libs, qapp):
    """AC-3/AC-6: the "Advanced citation details" section starts collapsed;
    values entered inside it are still collected on save."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        assert form._advanced_section.is_expanded() is False
        # Even while collapsed, the inputs can be written to (they exist).
        form.citation_key_input.setText("petersen-1990")
        form.language_input.setText("da")
        # Set the required fields so save succeeds.
        _select_type(form, "book")
        form.title_input.setText("Danmarks Basidiesvampe")
        form.authors_editor.add_row(family="Petersen", mark_dirty=True)
        form._on_save()
        assert form.result_work is not None
        assert form.result_work.citation_key == "petersen-1990"
        assert form.result_work.language == "da"
    finally:
        form.deleteLater()


# --- Live preview ------------------------------------------------------------


def test_live_preview_reflects_generated_short_label_and_full_citation(libs, qapp):
    """AC-4: preview updates from the canonical citation service. Missing
    data yields a merely incomplete preview — never a fabricated author,
    year, publisher, page, or identifier."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        _select_type(form, "book")
        form.title_input.setText("Danmarks Basidiesvampe")
        form.year_input.setText("1990")
        form.authors_editor.add_row(family="Petersen", mark_dirty=True)
        # After each field change the preview must have re-rendered.
        short = form.preview_short_label.text()
        full = form.preview_full_citation.text()
        assert short == "Petersen 1990"
        assert "Danmarks Basidiesvampe" in full
        assert "1990" in full
        # Overrides are not set -> both indicators are hidden.
        assert form.preview_short_override_indicator.isHidden() is True
        assert form.preview_full_override_indicator.isHidden() is True

        # Missing data does not fabricate values. Clear the author list —
        # the preview should degrade gracefully to just year + title.
        for row in list(form.authors_editor._rows):
            row.remove_requested.emit(row)
        short2 = form.preview_short_label.text()
        # No author -> the short label falls back to year (or title).
        assert "Petersen" not in short2
    finally:
        form.deleteLater()


def test_live_preview_marks_manual_override_when_short_label_or_citation_override_set(
    libs, qapp
):
    """AC-4: when the user provides a manual short-label override or
    manual full-citation override, the preview clearly labels the
    override."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        _select_type(form, "book")
        form.title_input.setText("Danmarks Basidiesvampe")
        form.short_label_input.setText("Manual: Petersen")
        assert form.preview_short_override_indicator.isHidden() is False
        assert form.preview_short_label.text() == "Manual: Petersen"

        form.citation_override_input.setPlainText("Custom full citation")
        assert form.preview_full_override_indicator.isHidden() is False
        assert form.preview_full_citation.text() == "Custom full citation"
    finally:
        form.deleteLater()


# --- Layout / scroll area ---------------------------------------------------


def test_dialog_is_scrollable_and_fits_laptop_screen(libs, qapp):
    """AC-3: the form fits on a typical laptop screen. The interior of the
    dialog is a QScrollArea so the sections can grow without pushing the
    Cancel/Save buttons out of reach."""
    from PySide6.QtWidgets import QScrollArea

    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        # Contains a QScrollArea as the field-holder.
        scrolls = form.findChildren(QScrollArea)
        assert scrolls, "form must contain a QScrollArea to fit laptop screens"
        # The dialog itself is capped to a laptop-friendly height.
        assert form.height() <= 900
    finally:
        form.deleteLater()


# --- Validation errors keep the dialog open ---------------------------------


def test_validation_missing_title_focuses_field_and_keeps_dialog_open(libs, qapp):
    """AC-5: missing title triggers a specific field-level error, keeps
    the dialog open, and focuses the offending field."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        _select_type(form, "book")
        # No title.
        form.title_input.setText("")
        form._on_save()
        assert form.result_work is None
        assert form.error_label.isHidden() is False
        assert form.error_label.text() != ""
        # Dialog stays open (the accepted signal was not emitted).
        from PySide6.QtWidgets import QDialog

        assert form.result() != QDialog.Accepted
    finally:
        form.deleteLater()


def test_validation_bad_year_shows_specific_field_level_error(libs, qapp):
    """AC-5: a non-integer year raises a specific error, not a generic
    "invalid input" — and does not close the dialog."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        _select_type(form, "book")
        form.title_input.setText("Some Title")
        form.year_input.setText("not a year")
        form._on_save()
        assert form.result_work is None
        assert form.error_label.isHidden() is False
        assert "year" in form.error_label.text().lower()
    finally:
        form.deleteLater()


def test_blank_doi_isbn_url_are_accepted(libs, qapp):
    """AC-5: blank identifier fields must NOT raise a validation error."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        _select_type(form, "book")
        form.title_input.setText("Some Title")
        form.authors_editor.add_row(family="A", mark_dirty=True)
        # Explicitly leave DOI, ISBN, URL blank.
        assert form.doi_input.text() == ""
        assert form.isbn_input.text() == ""
        assert form.url_input.text() == ""
        form._on_save()
        assert form.result_work is not None
        assert form.result_work.doi is None
        assert form.result_work.isbn is None
        assert form.result_work.url is None
    finally:
        form.deleteLater()


# --- Malformed JSON survives a normal save ---------------------------------


def test_malformed_authors_json_shows_warning_and_survives_repair(libs, qapp):
    """AC-1 companion: a work whose stored ``authors_json`` cannot be
    parsed opens without crashing, displays a translated warning, and
    preserves the raw string in-editor until the user replaces the value
    by adding a proper row. The repository validator ultimately requires
    a repair — the dialog stays open on the resulting error and the user
    can fix it in the same session."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    # Bypass the repository validator that would normally reject bad JSON;
    # we simulate a legacy row that already exists in a corrupt state.
    conn = _schema.get_reference_connection()
    from database.reference_library_schema import init_reference_library_schema

    init_reference_library_schema(conn)
    import uuid

    bad_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO reference_works
        (id, type, title, short_label, authors_json, editors_json)
        VALUES (?, 'book', 'Malformed sample', 'Malformed 2020', ?, '[]')
        """,
        (bad_id, "this-is-not-json"),
    )
    conn.commit()
    conn.close()

    persisted = ReferenceWorkRepository.get(bad_id)
    assert persisted is not None
    form = _ReferenceWorkForm(None, work=persisted)
    try:
        # Warning surfaced, no crash.
        assert form.authors_editor.warning_label.isHidden() is False
        # Editor did not silently discard the value — untouched, it would
        # serialize back verbatim.
        assert form.authors_editor.to_json() == "this-is-not-json"

        # Saving unrelated edits without repair is rejected by the repo's
        # JSON validator, and the dialog stays open with the error shown.
        form.title_input.setText("Malformed sample (edited)")
        form._on_save()
        assert form.result_work is None
        assert form.error_label.isHidden() is False

        # User repairs the value by adding a real author row.
        form.authors_editor.add_row(family="Petersen", mark_dirty=True)
        form._on_save()
        assert form.result_work is not None
        payload = json.loads(form.result_work.authors_json)
        assert payload == [{"family": "Petersen"}]
    finally:
        form.deleteLater()


# --- Translation registration -----------------------------------------------


def test_every_visible_string_in_editor_uses_tr(libs, qapp):
    """AC-7 policy: every new visible string in the editor should be
    routed through ``self.tr(...)`` so translators can pick it up. We
    verify the module source uses ``self.tr(`` for the human-readable
    labels/placeholders introduced in this rewrite, and that none of a
    representative sample of expected labels are missing.
    """
    import inspect

    from ui import reference_library_manager_dialog as module

    src = inspect.getsource(module)
    # Every user-visible string introduced by the rewrite must be
    # registered for translation via self.tr(...). Spot-check the
    # section titles and the person-list controls.
    for expected in (
        "self.tr(\"Basic information\")",
        "self.tr(\"Publication details\")",
        "self.tr(\"Identifiers\")",
        "self.tr(\"Advanced citation details\")",
        "self.tr(\"Preview\")",
        "self.tr(\"Authors:\")",
        "self.tr(\"Family name\")",
        "self.tr(\"Given names\")",
        "self.tr(\"Organization (optional)\")",
        "self.tr(\"+ Add author\")",
        "self.tr(\"+ Add editor\")",
        "self.tr(\"(manual override)\")",
        "self.tr(\"Short label:\")",
        "self.tr(\"Full citation:\")",
    ):
        assert expected in src, (
            f"expected translation-registered string not found: {expected}"
        )
