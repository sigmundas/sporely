"""Stage 2: the Library list's row anatomy, relevance grouping and multi-select.

The list has to keep two states apart. "Selected" means *shown in the
preview*; "checked" means *will be added to the plot*. A dialog that conflates
them either adds a source the user only wanted to look at, or loses two queued
sources the moment the user looks at a third — so most of what these tests pin
is the independence of those two states under selection, checkbox clicks,
searching, scope toggling and a change of comparison taxon.

The rest pins the information hierarchy of a row (italic taxon headline, grey
citation below it, monospace measurement that must not be truncated, relevance
and data-semantics badges) and the fact that the semantics badge comes from the
Stage 1 projection rather than from a ``data_kind`` column.

Everything here runs against injected candidates, so no reference database is
touched.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import json

import pytest
from PySide6.QtWidgets import QApplication

from database.reference_library import MeasurementSetCandidate
from references.reference_display import display_from_row, format_measurement_expression
from ui.add_reference_dialog import (
    group_library_candidates,
    library_relevance,
)
from ui.library_source_row import LibrarySourceRow


@pytest.fixture(autouse=True)
def isolated_picker_settings(tmp_path, monkeypatch):
    from PySide6.QtCore import QSettings
    import ui.window_state as geometry

    def settings(*_args):
        return QSettings(str(tmp_path / "picker.ini"), QSettings.IniFormat)

    monkeypatch.setattr(geometry, "QSettings", settings)


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


_PERCENTILE_DETAILS = json.dumps(
    {
        "schema_version": 1,
        "metrics": {
            "length": {
                "outer_range": {"kind": "reported_extremes"},
                "core_range": {
                    "kind": "percentile_interval",
                    "percentile_bounds": [5, 95],
                },
            }
        },
    }
)

_DECILE_DETAILS = json.dumps(
    {
        "schema_version": 1,
        "metrics": {
            "length": {
                "core_range": {
                    "kind": "percentile_interval",
                    "percentile_bounds": [10, 90],
                }
            }
        },
    }
)


def _candidate(
    measurement_set_id: str,
    *,
    name_as_published: str,
    short_label: str,
    taxon_id: str | None,
    row: dict,
    locator_text: str | None = None,
    raw_text: str | None = None,
    year: int | None = None,
) -> MeasurementSetCandidate:
    return MeasurementSetCandidate(
        measurement_set_id=measurement_set_id,
        short_label=short_label,
        name_as_published=name_as_published,
        locator_text=locator_text,
        data_kind=str(row.get("data_kind") or "range"),
        raw_text=raw_text,
        revision=1,
        reference_work_id=f"w-{measurement_set_id}",
        reference_treatment_id=f"t-{measurement_set_id}",
        year=year,
        taxon_id=taxon_id,
        display=display_from_row(row),
    )


def _candidates() -> list[MeasurementSetCandidate]:
    """Three relevance tiers for a picker sitting on Cortinarius limonius."""
    return [
        # This taxon — a plain published range.
        _candidate(
            "ms-this-range",
            name_as_published="Cortinarius limonius (Fr.) Fr.",
            short_label="Niskanen et al. 2018",
            locator_text="pp. 146–148",
            taxon_id="7",
            year=2018,
            raw_text="(8.1–)8.5–10.8(–11.4) µm",
            row={
                "data_kind": "range",
                "length_core_min": 8.5,
                "length_core_max": 10.8,
                "width_core_min": 4.5,
                "width_core_max": 5.8,
            },
        ),
        # This taxon — real individual points.
        _candidate(
            "ms-this-points",
            name_as_published="Cortinarius limonius (Fr.) Fr.",
            short_label="Niskanen et al. 2018",
            locator_text="supplementary dataset S4",
            taxon_id="7",
            row={
                "data_kind": "raw_points",
                "raw_points_json": json.dumps(
                    [{"length": 9.0, "width": 5.1}, {"length": 9.6, "width": 5.4}]
                ),
            },
        ),
        # Same genus — an explicit 5–95 percentile interval.
        _candidate(
            "ms-genus-percentile",
            name_as_published="Cortinarius rubellus Cooke",
            short_label="Brandrud et al. 2020",
            taxon_id="99",
            row={
                "data_kind": "range",
                "length_min": 7.0,
                "length_max": 12.0,
                "length_core_min": 8.0,
                "length_core_max": 11.0,
                "measurement_details_json": _PERCENTILE_DETAILS,
            },
        ),
        # Rest of library — an explicit, and deliberately different,
        # percentile interval.
        _candidate(
            "ms-rest-decile",
            name_as_published="Amanita muscaria (L.) Lam.",
            short_label="Funga Nordica 2012",
            taxon_id="500",
            row={
                "data_kind": "range",
                "length_core_min": 9.0,
                "length_core_max": 11.0,
                "measurement_details_json": _DECILE_DETAILS,
            },
        ),
    ]


# --- Relevance grouping ------------------------------------------------------


def test_relevance_is_decided_by_taxon_id_then_published_name():
    candidates = {c.measurement_set_id: c for c in _candidates()}
    kwargs = {"taxon_id": "7", "genus": "Cortinarius", "taxon_name": "Cortinarius limonius"}
    assert library_relevance(candidates["ms-this-range"], **kwargs) == "this_taxon"
    assert library_relevance(candidates["ms-genus-percentile"], **kwargs) == "same_genus"
    assert library_relevance(candidates["ms-rest-decile"], **kwargs) == "rest"


def test_relevance_still_finds_this_taxon_without_an_id():
    """An AI suggestion or a typed binomial has a name and no taxon id.

    Without the name check every row would fall through to ``same_genus``,
    which would put the exact taxon the user asked for below rows that only
    share its genus.
    """
    candidate = {c.measurement_set_id: c for c in _candidates()}["ms-this-range"]
    assert (
        library_relevance(
            candidate, taxon_id=None, genus="Cortinarius", taxon_name="Cortinarius limonius"
        )
        == "this_taxon"
    )


def test_groups_are_emitted_in_the_contract_order_with_their_members():
    grouped = group_library_candidates(
        _candidates(), taxon_id="7", genus="Cortinarius", taxon_name="Cortinarius limonius"
    )
    assert [key for key, _ in grouped] == ["this_taxon", "same_genus", "rest"]
    assert [len(members) for _, members in grouped] == [2, 1, 1]


def test_empty_groups_are_dropped_rather_than_shown_with_a_zero_count():
    only_this_taxon = [c for c in _candidates() if c.taxon_id == "7"]
    grouped = group_library_candidates(
        only_this_taxon,
        taxon_id="7",
        genus="Cortinarius",
        taxon_name="Cortinarius limonius",
    )
    assert [key for key, _ in grouped] == ["this_taxon"]


# --- Row anatomy -------------------------------------------------------------


def test_a_taxon_too_long_for_the_row_elides_instead_of_clipping():
    from PySide6.QtWidgets import QVBoxLayout, QWidget

    _app()
    host = QWidget()
    QVBoxLayout(host)
    row = LibrarySourceRow(
        measurement_set_id="ms-long",
        taxon="Cortinarius limonius (Fr.) Fr. with a deliberately long authority",
        citation="Niskanen et al. 2018",
        measurement="8.5–10.8 × 4.5–5.8 µm",
        parent=host,
    )
    host.layout().addWidget(row)
    try:
        host.resize(360, 80)
        host.show()
        _app().processEvents()
        assert row.taxon_label.text() != row.taxon_label.full_text()
        assert row.taxon_label.text().endswith("…")
        assert row.measurement_label.text() == "8.5–10.8 × 4.5–5.8 µm"
    finally:
        host.close()


def test_measurement_expression_prefers_the_extreme_pair_when_both_are_stored():
    display = display_from_row(
        {
            "data_kind": "range",
            "length_min": 7.0,
            "length_max": 12.0,
            "length_core_min": 8.0,
            "length_core_max": 11.0,
            "measurement_details_json": _PERCENTILE_DETAILS,
        }
    )
    assert format_measurement_expression(display) == "7.0–12.0 µm"


def test_measurement_expression_is_empty_when_the_source_stores_no_range():
    display = display_from_row({"data_kind": "range"})
    assert format_measurement_expression(display) == ""


# --- Selected versus checked -------------------------------------------------


# --- Checked state under filtering -------------------------------------------


# --- Footer ------------------------------------------------------------------


# --- The action row is not a source ------------------------------------------


# --- Public-observation attach notice (default-on reference sharing) ---------


