"""Referansebibliotek and its sharing dialogs say things in their bottom bars (stage 10e1).

Failures and results that were message boxes are notices in each dialog's
bottom bar (errors stay until dismissed, the error under Details). The
shared-contributions dialog closes after a copy, so Referansebibliotek, which
opened it, says it was copied. The details pane keeps identifiers (UUID,
revision, citation key, taxon id) behind a closed “Technical details”.

Real dialogs on an isolated library database; the cloud is a stand-in.
"""
from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QDialog, QMessageBox

import ui.curated_reference_catalogue_dialog as catalogue
import ui.reference_library_manager_dialog as manager_module
import ui.reference_sharing_dialogs as rsd
from database.curated_reference_forks import normalize_curated_bundle
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ReferenceLibraryError,
    ReferenceWorkRepository,
    TaxonTreatmentRepository,
)
from tests.test_curated_reference_forks import shared_row
from tests.test_reference_library_manager_dialog import _seed_work_treatment, libs, qapp  # noqa: F401
from tests.test_reference_sharing_default_on_desktop import FakeClient, _set
from utils import cloud_sync


def _notice(dialog):
    return dialog._hint_controller._notice


def _manager(**kwargs):
    return manager_module.ReferenceLibraryManagerDialog(None, **kwargs)


def test_publications_that_fail_to_load_are_said_in_the_bar(libs, qapp, monkeypatch):  # noqa: F811
    def broken(*_args, **_kwargs):
        raise ReferenceLibraryError("no such table: reference_works")

    monkeypatch.setattr(ReferenceWorkRepository, "search", broken)
    dialog = _manager()
    try:
        notice = _notice(dialog)
        assert notice.tone == "error" and notice.text == "Could not load the publications."
        assert notice.details == "no such table: reference_works"
        assert dialog.hint_bar._details_btn.isVisibleTo(dialog)
    finally:
        dialog.deleteLater()


def test_treatments_that_fail_to_load_are_said_in_the_bar(libs, qapp, monkeypatch):  # noqa: F811
    work, _treatment = _seed_work_treatment(libs)
    dialog = _manager()
    try:
        def broken(*_args, **_kwargs):
            raise ReferenceLibraryError("database is locked")

        monkeypatch.setattr(TaxonTreatmentRepository, "list_for_work", broken)
        dialog.refresh_works(select_id=work.id)
        notice = _notice(dialog)
        assert notice.text == "Could not load the treatments." and notice.details == "database is locked"
    finally:
        dialog.deleteLater()


def test_identifiers_are_behind_technical_details(libs, qapp):  # noqa: F811
    work, treatment = _seed_work_treatment(libs)
    ReferenceWorkRepository.update(work.id, {"citation_key": "petersen-1990"})
    ms = MeasurementSetRepository.create(
        MeasurementSet(id="", taxon_treatment_id=treatment.id, character="spore_size", data_kind="range",
                       length_core_min=8.0, length_core_max=10.0, width_core_min=5.0, width_core_max=6.0)
    )
    dialog = _manager()
    try:
        dialog.refresh_works(select_id=work.id)
        details, technical = dialog.detail_view.toPlainText(), dialog.technical_label.text()
        assert "Title: Danmarks Basidiesvampe" in details
        for hidden in ("UUID", "Revision", "Citation key", work.id):
            assert hidden not in details
        assert f"UUID: {work.id}" in technical and "Citation key: petersen-1990" in technical
        assert dialog.technical_section.isVisibleTo(dialog) and not dialog.technical_section.is_expanded()

        dialog._refresh_hierarchy_for_current_work(select_set_id=ms.id)
        assert dialog.current_selection_kind() == "measurement_set"
        assert ms.id not in dialog.detail_view.toPlainText()
        assert f"UUID: {ms.id}" in dialog.technical_label.text()

        dialog._clear_detail()
        assert not dialog.technical_section.isVisibleTo(dialog)
    finally:
        dialog.deleteLater()


def test_a_treatments_taxon_id_is_a_technical_detail(libs, qapp):  # noqa: F811
    work, treatment = _seed_work_treatment(libs)
    dialog = _manager()
    try:
        dialog._current_work = ReferenceWorkRepository.get(work.id)
        dialog._render_treatment_detail(treatment.__class__(**{**treatment.__dict__, "taxon_id": 4242}))
        assert "4242" not in dialog.detail_view.toPlainText()
        assert "Taxon id: 4242" in dialog.technical_label.text()
    finally:
        dialog.deleteLater()


# --- The shared contributions dialog ----------------------------------------


def _bundle():
    return normalize_curated_bundle(shared_row([]))


@pytest.fixture
def no_catalogue_load(monkeypatch):
    """The catalogue's background load is not started: rows are given directly."""
    monkeypatch.setattr(catalogue.SharedReferenceCatalogueDialog, "_start_load", lambda self: None)


def test_a_copied_contribution_is_said_in_referansebiblioteket(libs, qapp, monkeypatch, no_catalogue_load):  # noqa: F811
    """The real catalogue copies and closes; the library says so in its bar."""
    monkeypatch.setattr(
        catalogue, "copy_curated_bundle_to_personal_library",
        lambda _bundle: SimpleNamespace(created=True, reference_measurement_set_id="set-1"),
    )

    def choose_and_copy(self):
        self._loaded((_bundle(),))
        self.table.selectRow(0)
        self._copy_selected()
        return self.result()

    monkeypatch.setattr(catalogue.SharedReferenceCatalogueDialog, "exec", choose_and_copy)
    dialog = _manager(cloud_client=object(), sporely_taxon_id=2_100_000_081)
    try:
        dialog._on_copy_curated_clicked()
        notice = _notice(dialog)
        assert notice.tone == "success"
        assert notice.text == "The contribution was copied to your personal library."
    finally:
        dialog.deleteLater()


def test_a_failed_copy_is_said_in_the_catalogue(qapp, monkeypatch, no_catalogue_load):  # noqa: F811
    def broken(_bundle):
        raise RuntimeError("UNIQUE constraint failed")

    monkeypatch.setattr(catalogue, "copy_curated_bundle_to_personal_library", broken)
    dialog = catalogue.SharedReferenceCatalogueDialog(None, cloud_client=object(), sporely_taxon_id=2_100_000_081)
    try:
        dialog._loaded((_bundle(),))
        dialog.table.selectRow(0)
        dialog._copy_selected()
        notice = _notice(dialog)
        assert notice.tone == "error" and notice.text == "Could not copy the reference."
        assert notice.details == "UNIQUE constraint failed"
        assert dialog.result() != QDialog.Accepted and dialog.copy_result_message == ""
    finally:
        dialog.deleteLater()


def test_the_catalogues_close_button_is_translatable(qapp, no_catalogue_load):  # noqa: F811
    from PySide6.QtWidgets import QDialogButtonBox

    dialog = catalogue.SharedReferenceCatalogueDialog(None, cloud_client=object(), sporely_taxon_id=2_100_000_081)
    try:
        box = dialog.findChild(QDialogButtonBox)
        assert box.standardButtons() == QDialogButtonBox.NoButton
        assert [b.text() for b in box.buttons() if box.buttonRole(b) == QDialogButtonBox.RejectRole] == ["Close"]
    finally:
        dialog.deleteLater()


def test_a_copy_whose_list_fails_to_reload_says_both(libs, qapp, monkeypatch, no_catalogue_load):  # noqa: F811
    """Codex: the success notice replaced the reload's error."""
    monkeypatch.setattr(
        catalogue, "copy_curated_bundle_to_personal_library",
        lambda _bundle: SimpleNamespace(created=True, reference_measurement_set_id="set-1"),
    )

    def choose_and_copy(self):
        def broken(*_args, **_kwargs):
            raise ReferenceLibraryError("database is locked")

        monkeypatch.setattr(ReferenceWorkRepository, "search", broken)  # the reload after copying
        self._loaded((_bundle(),))
        self.table.selectRow(0)
        self._copy_selected()
        return self.result()

    monkeypatch.setattr(catalogue.SharedReferenceCatalogueDialog, "exec", choose_and_copy)
    dialog = _manager(cloud_client=object(), sporely_taxon_id=2_100_000_081)
    try:
        dialog._on_copy_curated_clicked()
        notice = _notice(dialog)
        assert notice.tone == "error"
        assert notice.text.startswith("The contribution was copied to your personal library.")
        assert "Could not load the publications." in notice.text and notice.details == "database is locked"
    finally:
        dialog.deleteLater()


# --- My shared references ----------------------------------------------------


def test_a_failed_sharing_action_shows_the_error_under_details(qapp, monkeypatch):  # noqa: F811
    client = FakeClient([_set("a", "shared")], action_result=cloud_sync.CloudSyncError("offline"))
    dialog = rsd.MySharedReferencesDialog(client)
    try:
        monkeypatch.setattr(dialog, "confirm_stop_sharing", lambda: True)
        monkeypatch.setattr(QMessageBox, "exec", lambda *_a: pytest.fail("no message box"))
        dialog.select_row(0)
        dialog.stop_btn.click()
        notice = _notice(dialog)
        assert notice.tone == "error" and notice.text == "Could not reach Sporely Cloud."
        assert notice.details == "offline"
        assert dialog.hint_bar.isVisibleTo(dialog)
    finally:
        dialog.deleteLater()


def test_a_sharing_result_is_a_notice(qapp):  # noqa: F811
    client = FakeClient([_set("b", "stopped")])
    dialog = rsd.MySharedReferencesDialog(client)
    try:
        def again(set_id):
            client.calls.append(("share_again", set_id))
            client.sets[0].update(status="hidden", hidden_by_moderation=True)
            return {"status": "updated", "row": None}

        client.share_reference_set_again = again
        dialog.select_row(0)
        dialog.share_again_btn.click()
        notice = _notice(dialog)
        assert notice.tone == "warning" and "hidden by moderation" in notice.text
    finally:
        dialog.deleteLater()
