"""Stage 3: the manual-entry layout and its semantic preview.

The Enter-manually tab is one column (publication, then measurements), its
numeric surface is three metric rows by five plain inputs rather than a
table widget, and what the user pastes or types is drawn live through the
same comparison model every other source tab uses.

These tests pin the two things the redesign must not trade against each
other: the approved information hierarchy, and the statistical meaning the
parser established. A descriptor that survives into the grid but not into
the preview -- or a preview that invents a centre the source never printed
-- is the failure this module is here to catch.

No real database: the publication and measurement-set repositories are
monkeypatched to empty results, matching ``tests/test_reference_entry_editor``.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QGroupBox, QMessageBox, QTabWidget

from ui.reference_entry_editor import ReferenceEntryEditor
from ui.reference_preview_pane import ReferencePreviewPane

MEAN_COLUMN = 2

#: A bare published range: two typical bounds and nothing else. The source
#: printed no extremes and no centre.
PUBLISHED_RANGE = "8.4-13.0 x 5.1-7.2 um"

#: The ordinary literature notation: parenthesised extremes around a typical
#: range, still with no stated centre.
OUTER_AND_CORE = "(9.5-)9.8-11.3(-11.7) x (7.3-)8.0-9.4(-9.4) um"

#: A table that explicitly identifies its inner bounds as the 5th and 95th
#: percentiles, and reports mean and median as intervals.
EXPLICIT_5_95 = "\n".join(
    [
        "Spore\t(min) 5%-95% (max)\tmean\tmedian\tS.D.",
        "Length\t(7.5) 8.4-13.0 (13.2)\t9.2-11.7\t9.2-11.7\t0.600",
        "Width\t(5.0) 5.1-7.2 (7.6)\t5.6-6.7\t5.6-6.7\t0.280",
        "Q\t(1.30) 1.42-1.96 (2.07)\t1.55-1.78\t1.54-1.79\t0.095",
    ]
)


def _app() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


@pytest.fixture(autouse=True)
def isolated_editor_repositories(monkeypatch):
    import ui.reference_entry_editor as editor_mod

    monkeypatch.setattr(
        editor_mod.ReferenceWorkRepository, "list_recent", lambda limit=500: []
    )
    monkeypatch.setattr(
        editor_mod.ReferenceWorkRepository, "search", lambda query, limit=50: []
    )
    monkeypatch.setattr(
        editor_mod.TaxonTreatmentRepository, "list_for_work", lambda work_id: []
    )
    monkeypatch.setattr(QMessageBox, "warning", lambda *a, **k: None)
    monkeypatch.setattr(QMessageBox, "question", lambda *a, **k: QMessageBox.Yes)


@pytest.fixture()
def editor() -> ReferenceEntryEditor:
    _app()
    pane = ReferencePreviewPane(None)
    return ReferenceEntryEditor(
        None,
        "Hebeloma",
        "crustuliniforme",
        observation_id=42,
        sporely_taxon_id=7,
        preview_pane=pane,
    )


def _parse(editor: ReferenceEntryEditor, text: str) -> None:
    editor.measurement_paste_input.setText(text)
    editor._on_parse_measurement_clicked()


def _source(editor: ReferenceEntryEditor, metric: str):
    return editor._preview_pane.comparison_view.view().metric(metric).source


# --- Layout ------------------------------------------------------------------


def test_the_manual_editor_is_one_column_with_publication_first(editor):
    """Contract N23: paste-and-parse is the path, not one tab among three.

    The measurement tab bar put the min/max grid, the spore table and the
    Parmasto biometrics on equal footing; the approved hierarchy is
    publication, then measurements, in a single column.
    """
    assert not hasattr(editor, "tabs")
    assert editor.findChild(QTabWidget) is None

    layout = editor.layout()
    order = [layout.itemAt(i).widget() for i in range(layout.count())]
    group_titles = [w.title() for w in order if isinstance(w, QGroupBox)]
    assert group_titles[0] == "Publication"
    assert group_titles[1] == "Measurements"


def test_secondary_measurement_paths_stay_reachable_but_closed(editor):
    """Individual spore rows and Parmasto biometrics are still supported.

    They are not the path a published range takes, so they start collapsed
    rather than competing with the grid -- but removing them would drop real
    functionality, which this stage does not authorise.
    """
    assert editor._spore_section.is_expanded() is False
    assert editor._parmasto_section.is_expanded() is False
    assert editor.spore_table is not None
    assert set(editor.parmasto_inputs) >= {"parmasto_length_mean", "parmasto_q_mean"}

    editor._spore_section.set_expanded(True)
    assert editor._spore_section.is_expanded() is True
    assert editor.spore_table.isVisibleTo(editor) is True


def test_the_no_taxon_warning_tracks_a_real_identity_gap(editor):
    """Contract N26: the amber notice is a real gap, not decoration."""
    # ``isVisibleTo`` rather than ``isVisible``: an unshown editor reports
    # every child hidden, which would pass the negative assertions for the
    # wrong reason.
    assert editor._no_taxon_notice_label.isVisibleTo(editor) is False

    editor.set_comparison_target(genus="Hebeloma", species="sp.", sporely_taxon_id=None)
    assert editor._no_taxon_notice_label.isVisibleTo(editor) is True

    editor.set_comparison_target(
        genus="Hebeloma", species="crustuliniforme", sporely_taxon_id=7
    )
    assert editor._no_taxon_notice_label.isVisibleTo(editor) is False


def test_name_as_published_and_locator_sit_with_the_publication(editor):
    publication_group = next(
        widget
        for widget in editor.findChildren(QGroupBox)
        if widget.title() == "Publication"
    )
    assert editor.name_as_published_input.parent() is publication_group
    assert editor.locator_input.parent() is publication_group


# --- Parse fills the grid ----------------------------------------------------


def test_parsing_a_published_range_fills_the_typical_columns_only(editor):
    """A bare published range states two bounds. It states nothing else.

    Neither extreme column may be filled from the typical bounds, and the
    mean column stays empty: a range midpoint is not a mean (contract N15).
    """
    _parse(editor, PUBLISHED_RANGE)

    assert editor.measurement_cell_text(0, 0) == ""
    assert editor.measurement_cell_text(0, 1) == "8.40"
    assert editor.measurement_cell_text(0, MEAN_COLUMN) == ""
    assert editor.measurement_cell_text(0, 3) == "13.00"
    assert editor.measurement_cell_text(0, 4) == ""

    length = _source(editor, "length")
    assert length.outer is None
    assert (length.core.low, length.core.high) == pytest.approx((8.4, 13.0))
    assert length.centres == ()


def test_parsing_outer_and_core_fills_and_draws_both(editor):
    _parse(editor, OUTER_AND_CORE)

    assert editor.measurement_cell_text(0, 0) == "9.50"
    assert editor.measurement_cell_text(0, 1) == "9.80"
    assert editor.measurement_cell_text(0, 3) == "11.30"
    assert editor.measurement_cell_text(0, 4) == "11.70"

    length = _source(editor, "length")
    assert (length.outer.low, length.outer.high) == pytest.approx((9.5, 11.7))
    assert length.outer.meaning == "reported_extremes"
    assert (length.core.low, length.core.high) == pytest.approx((9.8, 11.3))
    # Parenthesised extremes around a typical range say nothing about
    # percentiles (contract N12).
    assert length.core.meaning != "percentile_interval"
    assert length.core.percentile_bounds is None
    assert length.centres == ()


def test_explicit_5_95_data_stays_a_percentile_interval_in_the_preview(editor):
    """Contract N25: an explicitly stated 5-95% interval keeps that meaning.

    This is the one case where the inner band really is a percentile
    interval, and it must be distinguishable from the reported extremes
    around it.
    """
    _parse(editor, EXPLICIT_5_95)

    length = _source(editor, "length")
    assert length.core.meaning == "percentile_interval"
    assert length.core.percentile_bounds == pytest.approx((5.0, 95.0))
    assert (length.core.low, length.core.high) == pytest.approx((8.4, 13.0))
    assert length.outer.meaning == "reported_extremes"
    assert (length.outer.low, length.outer.high) == pytest.approx((7.5, 13.2))
    # The reported mean is an interval and is never collapsed to a midpoint.
    assert length.centre("mean").interval == pytest.approx((9.2, 11.7))
    assert length.centre("median").interval == pytest.approx((9.2, 11.7))


# --- Editing after a parse ---------------------------------------------------


def test_editing_a_bound_keeps_the_descriptor_the_source_stated(editor):
    """The grid edits the numbers; it does not retract their meaning.

    Correcting a transcription slip in a 5-95% bound must leave it a 5-95%
    bound -- dropping the descriptor is a separate, deliberate action
    (the "Correct interpretation" menu).
    """
    _parse(editor, EXPLICIT_5_95)
    editor.set_measurement_cell_text(0, 1, "8.60")

    length = _source(editor, "length")
    assert (length.core.low, length.core.high) == pytest.approx((8.6, 13.0))
    assert length.core.meaning == "percentile_interval"
    assert length.core.percentile_bounds == pytest.approx((5.0, 95.0))


def test_emptying_a_described_pair_takes_its_descriptor_with_it(editor):
    """A descriptor must never outlive the numbers it describes."""
    _parse(editor, EXPLICIT_5_95)
    editor.set_measurement_cell_text(0, 1, "")
    editor.set_measurement_cell_text(0, 3, "")

    length = _source(editor, "length")
    assert length.core is None
    assert editor._measurement_content.details.metrics["length"].core_range is None


def test_swapping_l_and_w_moves_numbers_and_descriptors_together(editor):
    """Contract N15: a tag left behind would describe the other dimension.

    Length's inner band is an explicit 5-95% interval and width's is too,
    but their *numbers* differ, so the swap is only correct if each
    descriptor lands on the pair it actually describes.
    """
    _parse(editor, EXPLICIT_5_95)
    editor._on_swap_lw_clicked()

    assert editor.measurement_cell_text(0, 1) == "5.10"
    assert editor.measurement_cell_text(1, 1) == "8.40"

    length = _source(editor, "length")
    width = _source(editor, "width")
    assert (length.core.low, length.core.high) == pytest.approx((5.1, 7.2))
    assert length.core.meaning == "percentile_interval"
    assert length.centre("mean").interval == pytest.approx((5.6, 6.7))
    assert (width.core.low, width.core.high) == pytest.approx((8.4, 13.0))
    assert width.core.meaning == "percentile_interval"
    assert width.centre("mean").interval == pytest.approx((9.2, 11.7))


# --- The preview follows the entry -------------------------------------------


def test_the_comparison_axes_do_not_move_while_the_entry_changes(editor):
    """Contract N19: the axes are frozen for the session.

    A preview that rescaled as the user typed would silently invalidate the
    shape they had just read.
    """
    _parse(editor, PUBLISHED_RANGE)
    before = {
        metric: (
            editor._preview_pane.comparison_view.view().metric(metric).domain.low,
            editor._preview_pane.comparison_view.view().metric(metric).domain.high,
        )
        for metric in ("length", "width", "q")
    }

    _parse(editor, EXPLICIT_5_95)
    editor.set_measurement_cell_text(0, 4, "40.00")

    after = {
        metric: (
            editor._preview_pane.comparison_view.view().metric(metric).domain.low,
            editor._preview_pane.comparison_view.view().metric(metric).domain.high,
        )
        for metric in ("length", "width", "q")
    }
    assert after == before


def test_an_empty_grid_shows_the_baseline_rather_than_a_source(editor):
    """Contract N21: nothing entered is not a grid of dashes."""
    view = editor._preview_pane.comparison_view.view()
    assert view is None or view.has_source is False


def test_the_raw_spores_tab_never_invents_rows_for_a_range(editor):
    """Contract N22: a published range has no per-spore measurements."""
    _parse(editor, EXPLICIT_5_95)

    text = editor._preview_pane.raw_spores_text.toPlainText()
    assert "were not published for this source" in text
    assert "9.2" not in text


# --- The Stage 3 semantic boundary -------------------------------------------


def test_the_footer_will_not_offer_to_add_an_entry_that_cannot_be_stored(editor):
    """The picker's Add-to-plot must not invite an unstorable save.

    The comparison pane is drawing the source's explicit 5-95% interval, and
    this version cannot store that. Offering the button and then refusing is
    the "moved refusal" the rollout note warns about; the button is simply
    not offered.
    """
    _parse(editor, EXPLICIT_5_95)

    assert editor.is_ready_to_submit() is False
    assert editor._blocked_projection_losses()


def test_the_preview_and_the_refusal_describe_the_same_entry(editor):
    """The picture and the block must not disagree.

    The whole failure mode this stage names is a preview that is right while
    the stored row is wrong. If the comparison shows a percentile interval,
    the refusal must be about that percentile interval.
    """
    _parse(editor, EXPLICIT_5_95)

    core = _source(editor, "length").core
    assert core.meaning == "percentile_interval"
    kinds = {kind for _metric, kind in editor._blocked_projection_losses()}
    assert "percentile_interval" in kinds


def test_an_ordinary_pasted_length_and_width_range_is_not_blocked(editor):
    """The block stays as narrow as the storage format allows.

    ``(extreme-)typical-typical(-extreme)`` for length and width is exactly
    what the v1 outer and core columns hold, so it saves untouched.
    """
    _parse(editor, OUTER_AND_CORE)

    assert editor._blocked_projection_losses() == []
    assert editor.is_ready_to_submit() is True


def test_a_q_typical_range_blocks_because_v1_has_no_q_core_columns(editor):
    """Q is the one metric whose inner pair has nowhere to go.

    Length and width have ``*_core_min``/``*_core_max``; Q does not, until
    the enhanced gate opens. So the same notation that saves without a Q
    range is refused with one -- not a preference about wording, but the
    only way not to drop or relabel numbers the source printed.
    """
    _parse(editor, OUTER_AND_CORE + ", Q = 1.2-1.3")

    kinds = {kind for _metric, kind in editor._blocked_projection_losses()}
    assert kinds == {"q_core_pair"}
    assert editor.is_ready_to_submit() is False


def test_q_extremes_plus_a_q_typical_range_block_on_real_numeric_loss(editor):
    """Both Q ranges stated: the inner one would vanish, not be relabelled.

    The v1 fallback prefers the outer pair, so 1.2-1.8 has no column left
    to land in. Two numbers the user can see on screen would simply stop
    existing.
    """
    _parse(editor, "10-12 x 5-6, Q = (1.1-)1.2-1.8(-1.9), Qm = 1.5")

    assert editor.measurement_cell_text(2, 1) == "1.20"
    assert editor.measurement_cell_text(2, 3) == "1.80"
    kinds = {kind for _metric, kind in editor._blocked_projection_losses()}
    assert "q_core_pair" in kinds
    assert editor.is_ready_to_submit() is False


def test_a_bare_published_range_is_not_blocked(editor):
    _parse(editor, PUBLISHED_RANGE)

    assert editor._blocked_projection_losses() == []
    assert editor.is_ready_to_submit() is True
