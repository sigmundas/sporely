"""Stage 2c: relationship labels in the desktop catalogue and fork dialog."""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication

from database.curated_reference_forks import normalize_curated_bundle
from tests.test_curated_reference_forks import shared_row
from ui import curated_reference_catalogue_dialog as catalogue
from ui.curated_reference_catalogue_dialog import SharedReferenceCatalogueDialog, relationship_label


@pytest.fixture(scope="module")
def qapp():
    return QApplication.instance() or QApplication([])


@pytest.mark.parametrize("roles,label", [
    ([], ""),
    (["supports_identification"], "Supports"),
    (["contradicts"], "Contradicts"),
    (["compared"], "Compared"),
    (["compared", "supports_identification"], "Supports · Compared"),
    (["compared", "contradicts", "supports_identification"], "Supports · Contradicts · Compared"),
    (["compared", "contradicts"], "Contradicts · Compared"),
])
def test_label_rule_order_and_empty(roles, label):
    assert relationship_label(roles) == label
    assert relationship_label(tuple(reversed(roles))) == label


def _dialog(monkeypatch, roles):
    monkeypatch.setattr(SharedReferenceCatalogueDialog, "_start_load", lambda self: None)
    dialog = SharedReferenceCatalogueDialog(None, cloud_client=object(), sporely_taxon_id=2_100_000_081)
    dialog._loaded((normalize_curated_bundle(shared_row(roles)),))
    return dialog


def test_catalogue_shows_contradicts_prominently_as_current_relationship(qapp, monkeypatch):
    dialog = _dialog(monkeypatch, ["compared", "contradicts"])
    cell = dialog.table.item(0, 5)
    assert dialog.table.horizontalHeaderItem(5).text() == "Current relationship"
    assert cell.text() == "Contradicts · Compared"
    assert cell.font().bold()
    dialog.table.selectRow(0)
    assert not dialog.relationship_label.isHidden()
    text = dialog.relationship_label.text()
    assert text.startswith("Contradicts the identification.")
    assert "currently" in text and "Russula publicata" in text


def test_catalogue_empty_roles_show_no_label_never_supports(qapp, monkeypatch):
    dialog = _dialog(monkeypatch, [])
    assert dialog.table.item(0, 5).text() == ""
    dialog.table.selectRow(0)
    assert dialog.relationship_label.isHidden()
    assert "Supports" not in dialog.relationship_label.text()


def test_catalogue_supporting_row_is_not_bold(qapp, monkeypatch):
    dialog = _dialog(monkeypatch, ["supports_identification"])
    assert not dialog.table.item(0, 5).font().bold()
    dialog.table.selectRow(0)
    assert dialog.relationship_label.text() == (
        "Current relationship on the contributor's public observations: Supports")
