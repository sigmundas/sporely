"""Owner UI for consent-gated public reference sharing (Stage 2b).

Two dialogs:

* :class:`ReferenceShareConsentDialog` -- "Share publicly..." for a reference
  set attached to an observation. It shows the server's active consent text
  verbatim and, only on an explicit confirm, calls
  ``share_reference_contribution_with_consent`` with the revisions the dialog
  displayed, the text's version and locale, and ``consent_client='desktop'``.
* :class:`MySharedReferencesDialog` -- lists the owner's contributions and
  offers "Stop sharing" (``withdraw_reference_contribution``).

Sync never shares: these dialogs are the only callers of the grant RPC.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from PySide6.QtCore import QCoreApplication, Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

CONSENT_CLIENT = "desktop"


def consent_locale_for_ui(ui_language: str | None) -> str:
    """``nb`` for a Norwegian UI, otherwise ``en``."""
    code = str(ui_language or "").strip().lower()
    if code.startswith(("nb", "no", "nn")):
        return "nb"
    return "en"


@dataclass(frozen=True)
class ReferenceShareRequest:
    """The source revisions the dialog displays and sends to the server."""

    source_measurement_set_id: str
    sporely_taxon_id: int
    work_revision: int
    treatment_revision: int
    measurement_set_revision: int
    set_label: str = ""
    species_label: str = ""


def build_share_request_for_use(use_id: str, observation_id: int) -> ReferenceShareRequest:
    """Build the request from the local attached use and its library rows.

    Raises ``ValueError`` with a user-facing message when the set cannot be
    offered for sharing yet. The revisions are the local library revisions,
    which sync mirrors to the cloud; the server rejects any mismatch.
    """
    from database.models import ObservationDB
    from database.reference_library import (
        MeasurementSetRepository,
        ObservationReferenceUseRepository,
        ReferenceWorkRepository,
        TaxonTreatmentRepository,
    )
    from database.reference_sync_state import ReferenceCloudSyncStateRepository

    use = ObservationReferenceUseRepository.get(str(use_id))
    if use is None or (observation_id and int(use.observation_id) != int(observation_id)):
        raise ValueError(QCoreApplication.translate(
            "ReferenceSharing", "This reference is no longer attached to the observation."))
    measurement_set = MeasurementSetRepository.get(use.reference_measurement_set_id)
    treatment = (
        TaxonTreatmentRepository.get(measurement_set.taxon_treatment_id)
        if measurement_set is not None else None
    )
    work = (
        ReferenceWorkRepository.get(treatment.reference_work_id)
        if treatment is not None else None
    )
    if measurement_set is None or treatment is None or work is None:
        raise ValueError(QCoreApplication.translate(
            "ReferenceSharing", "This reference is no longer in your library."))
    if int(use.reference_revision) != int(measurement_set.revision):
        raise ValueError(QCoreApplication.translate(
            "ReferenceSharing",
            "The attached copy is older than the library version. Use "
            "\"Update from library\" first, then share."))
    observation = ObservationDB.get_observation(int(use.observation_id)) or {}
    try:
        taxon_id = int(observation.get("sporely_taxon_id") or 0)
    except (TypeError, ValueError):
        taxon_id = 0
    if taxon_id <= 0:
        raise ValueError(QCoreApplication.translate(
            "ReferenceSharing",
            "The observation must be identified to a Sporely species before "
            "its reference can be shared."))
    for entity_type, entity_id in (
        ("work", work.id), ("treatment", treatment.id),
        ("measurement_set", measurement_set.id),
    ):
        state = ReferenceCloudSyncStateRepository.get_library(entity_type, entity_id)
        if (
            state is None
            or state.remote_identity_state != "acknowledged"
            or state.sync_status != "clean"
        ):
            raise ValueError(QCoreApplication.translate(
                "ReferenceSharing",
                "Sync this reference to Sporely Cloud before sharing it."))
    return ReferenceShareRequest(
        source_measurement_set_id=str(measurement_set.id),
        sporely_taxon_id=taxon_id,
        work_revision=int(work.revision),
        treatment_revision=int(treatment.revision),
        measurement_set_revision=int(measurement_set.revision),
        set_label=" — ".join(
            part for part in (work.short_label, measurement_set.raw_text or "") if part
        ),
        species_label=str(treatment.name_as_published or ""),
    )


def _is_rate_limited_error(exc: Exception) -> bool:
    return "rate_limited" in str(exc) or "429" in str(exc)


def share_status_message(status: str) -> tuple[bool, str]:
    """(success, user message) for a grant RPC status."""
    status = str(status or "").strip()
    if status in ("created", "updated", "no_change"):
        return True, QCoreApplication.translate("ReferenceSharing", "This reference is now shared publicly.")
    if status in ("consent_required", "consent_text_unavailable", "consent_text_revoked"):
        return False, QCoreApplication.translate("ReferenceSharing", 
            "The sharing terms have changed or are not available right now. "
            "Nothing was shared."
        )
    if status == "revision_mismatch":
        return False, QCoreApplication.translate("ReferenceSharing", 
            "This reference changed since it was shown. The current version "
            "has been reloaded; please review it and confirm again."
        )
    if status in ("qualifying_use_required", "withdrawn_unqualified"):
        return False, QCoreApplication.translate("ReferenceSharing", 
            "This reference can only be shared while it is attached to an "
            "observation that is public (not a draft), whose spore data is "
            "public, and that is identified as the same species."
        )
    if status == "invalid_taxon":
        return False, QCoreApplication.translate("ReferenceSharing", 
            "This species cannot be used for public references. Only species "
            "in the Sporely taxonomy can be shared."
        )
    if status == "rate_limited":
        return False, QCoreApplication.translate("ReferenceSharing", "Too many requests. Please wait a moment and try again.")
    if status == "source_not_found_or_stale":
        return False, QCoreApplication.translate("ReferenceSharing", 
            "The cloud copy of this reference is missing or out of date. "
            "Sync with the cloud, update the reference from the library, and "
            "try again."
        )
    if status in ("source_out_of_bounds", "consent_scope_exceeded", "invalid_payload"):
        return False, QCoreApplication.translate("ReferenceSharing", "This reference cannot be shared in its current form.")
    if status == "account_unavailable":
        return False, QCoreApplication.translate("ReferenceSharing", "Your account cannot share references.")
    return False, QCoreApplication.translate("ReferenceSharing", "Sharing failed ({status}).").format(status=status or "?")


class ReferenceShareConsentDialog(QDialog):
    """Consent dialog that shows the server text verbatim and grants on confirm."""

    def __init__(
        self,
        cloud_client,
        request: ReferenceShareRequest,
        *,
        locale: str,
        reload_request: Callable[[], ReferenceShareRequest | None] | None = None,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._client = cloud_client
        self._request = request
        self._locale = locale
        self._reload_request = reload_request
        self._consent: dict | None = None
        self.result_status: str | None = None
        self.setWindowTitle(QCoreApplication.translate("ReferenceSharing", "Share reference publicly"))
        self.resize(560, 520)

        layout = QVBoxLayout(self)
        self.summary_label = QLabel(self)
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)
        self.text_view = QPlainTextEdit(self)
        self.text_view.setReadOnly(True)
        layout.addWidget(self.text_view, 1)
        self.status_label = QLabel(self)
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)

        self.buttons = QDialogButtonBox(self)
        self.share_btn = self.buttons.addButton(
            QCoreApplication.translate("ReferenceSharing", "Share publicly"), QDialogButtonBox.AcceptRole
        )
        self.cancel_btn = self.buttons.addButton(QDialogButtonBox.Cancel)
        self.share_btn.clicked.connect(self._on_share_clicked)
        self.cancel_btn.clicked.connect(self.reject)
        self.share_btn.setDefault(False)
        self.cancel_btn.setDefault(True)
        layout.addWidget(self.buttons)

        self._render_summary()
        self.load_consent_text()

    # --- State ---

    @property
    def request(self) -> ReferenceShareRequest:
        return self._request

    @property
    def consent(self) -> dict | None:
        return self._consent

    def _render_summary(self) -> None:
        r = self._request
        self.summary_label.setText(
            QCoreApplication.translate("ReferenceSharing", "Reference: {label}\nSpecies: {species}\nRevision: {revision}").format(
                label=r.set_label or r.source_measurement_set_id,
                species=r.species_label or str(r.sporely_taxon_id),
                revision=r.measurement_set_revision,
            )
        )

    def load_consent_text(self) -> None:
        self._consent = None
        self.share_btn.setEnabled(False)
        self.text_view.setPlainText("")
        try:
            result = self._client.get_reference_share_consent_text(self._locale)
        except Exception as exc:  # network / server error
            if _is_rate_limited_error(exc):
                self.status_label.setText(share_status_message("rate_limited")[1])
            else:
                self.status_label.setText(
                    QCoreApplication.translate("ReferenceSharing", "Could not reach Sporely Cloud: {error}").format(error=str(exc))
                )
            return
        if (
            not isinstance(result, dict)
            or result.get("status") != "ok"
            or not str(result.get("text") or "").strip()
            or result.get("version") is None
        ):
            self.status_label.setText(QCoreApplication.translate("ReferenceSharing", "Public sharing isn't available yet."))
            return
        self._consent = result
        # Verbatim: the exact server text, not reflowed or translated.
        self.text_view.setPlainText(str(result.get("text")))
        self.status_label.setText(
            QCoreApplication.translate("ReferenceSharing", "Read the terms above. Nothing is shared until you click "
               "\"Share publicly\".")
        )
        self.share_btn.setEnabled(True)

    # --- Grant ---

    def _on_share_clicked(self) -> None:
        if self._consent is None:
            return
        r = self._request
        try:
            result = self._client.share_reference_contribution_with_consent(
                r.source_measurement_set_id,
                int(r.sporely_taxon_id),
                int(r.work_revision),
                int(r.treatment_revision),
                int(r.measurement_set_revision),
                int(self._consent["version"]),
                str(self._consent.get("locale") or self._locale),
                CONSENT_CLIENT,
            )
        except Exception as exc:
            if _is_rate_limited_error(exc):
                self._show_failure("rate_limited")
            else:
                self.result_status = "network_error"
                QMessageBox.warning(
                    self,
                    self.windowTitle(),
                    QCoreApplication.translate("ReferenceSharing", "Could not reach Sporely Cloud: {error}").format(error=str(exc)),
                )
            return
        status = str((result or {}).get("status") or "") if isinstance(result, dict) else ""
        self.result_status = status
        ok, message = share_status_message(status)
        if ok:
            QMessageBox.information(self, self.windowTitle(), message)
            self.accept()
            return
        if status == "revision_mismatch":
            self._reload_after_mismatch()
        elif status in ("consent_required", "consent_text_unavailable", "consent_text_revoked"):
            self.load_consent_text()
        QMessageBox.warning(self, self.windowTitle(), message)

    def _show_failure(self, status: str) -> None:
        self.result_status = status
        QMessageBox.warning(self, self.windowTitle(), share_status_message(status)[1])

    def _reload_after_mismatch(self) -> None:
        """Reload displayed revisions and require a fresh confirm."""
        if self._reload_request is not None:
            try:
                fresh = self._reload_request()
            except Exception:
                fresh = None
            if fresh is not None:
                self._request = fresh
                self._render_summary()
        self.load_consent_text()


def _withdrawal_reason_label(reason: str | None) -> str:
    labels = {
        "owner": QCoreApplication.translate("ReferenceSharing", "Stopped by you"),
        "use_detached": QCoreApplication.translate("ReferenceSharing", "Reference detached from the observation"),
        "taxon_changed": QCoreApplication.translate("ReferenceSharing", "Observation identification changed"),
        "observation_not_public": QCoreApplication.translate("ReferenceSharing", "Observation is no longer public"),
        "source_deleted": QCoreApplication.translate("ReferenceSharing", "Reference deleted"),
        "account_deleted": QCoreApplication.translate("ReferenceSharing", "Account deleted"),
        "consent_missing": QCoreApplication.translate("ReferenceSharing", "Shared without consent (withdrawn)"),
        "consent_scope_exceeded": QCoreApplication.translate("ReferenceSharing", "Changed beyond what you agreed to share"),
        "consent_text_revoked": QCoreApplication.translate("ReferenceSharing", "Sharing terms were withdrawn"),
    }
    text = str(reason or "").strip()
    return labels.get(text, text)


def _status_label(status: str | None) -> str:
    return {
        "shared": QCoreApplication.translate("ReferenceSharing", "Shared"),
        "withdrawn": QCoreApplication.translate("ReferenceSharing", "Withdrawn"),
    }.get(str(status or ""), str(status or ""))


class MySharedReferencesDialog(QDialog):
    """List the owner's shared references with "Stop sharing"."""

    COLUMNS = ("status", "species", "label", "revision", "shared", "withdrawn", "moderation", "reason")

    def __init__(self, cloud_client, parent=None) -> None:
        super().__init__(parent)
        self._client = cloud_client
        self._rows: list[dict] = []
        self.setWindowTitle(QCoreApplication.translate("ReferenceSharing", "My shared references"))
        self.resize(900, 420)

        layout = QVBoxLayout(self)
        self.status_label = QLabel(self)
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)
        self.table = QTableWidget(0, len(self.COLUMNS), self)
        self.table.setHorizontalHeaderLabels([
            QCoreApplication.translate("ReferenceSharing", "Status"), QCoreApplication.translate("ReferenceSharing", "Species"), QCoreApplication.translate("ReferenceSharing", "Reference"), QCoreApplication.translate("ReferenceSharing", "Revision"),
            QCoreApplication.translate("ReferenceSharing", "Shared"), QCoreApplication.translate("ReferenceSharing", "Withdrawn"), QCoreApplication.translate("ReferenceSharing", "Moderation"), QCoreApplication.translate("ReferenceSharing", "Withdrawal reason"),
        ])
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.itemSelectionChanged.connect(self._update_buttons)
        layout.addWidget(self.table, 1)

        row = QHBoxLayout()
        self.refresh_btn = QPushButton(QCoreApplication.translate("ReferenceSharing", "Refresh"), self)
        self.refresh_btn.clicked.connect(self.refresh)
        row.addWidget(self.refresh_btn)
        row.addStretch(1)
        self.stop_btn = QPushButton(QCoreApplication.translate("ReferenceSharing", "Stop sharing…"), self)
        self.stop_btn.clicked.connect(self._on_stop_clicked)
        row.addWidget(self.stop_btn)
        close_btn = QPushButton(QCoreApplication.translate("ReferenceSharing", "Close"), self)
        close_btn.clicked.connect(self.accept)
        row.addWidget(close_btn)
        layout.addLayout(row)

        self.refresh()

    @property
    def rows(self) -> list[dict]:
        return list(self._rows)

    def refresh(self) -> None:
        self._rows = []
        self.table.setRowCount(0)
        try:
            result = self._client.list_my_shared_reference_contributions()
        except Exception as exc:
            if _is_rate_limited_error(exc):
                self.status_label.setText(share_status_message("rate_limited")[1])
            else:
                self.status_label.setText(
                    QCoreApplication.translate("ReferenceSharing", "Could not reach Sporely Cloud: {error}").format(error=str(exc))
                )
            self._update_buttons()
            return
        if not isinstance(result, dict) or result.get("status") != "ok":
            status = result.get("status") if isinstance(result, dict) else ""
            self.status_label.setText(share_status_message(str(status or ""))[1])
            self._update_buttons()
            return
        self._rows = [r for r in (result.get("contributions") or []) if isinstance(r, dict)]
        self.table.setRowCount(len(self._rows))
        for index, item in enumerate(self._rows):
            label = item.get("source_short_label") or ""
            raw = item.get("source_raw_text") or ""
            if raw:
                label = f"{label} — {raw}" if label else raw
            values = (
                _status_label(item.get("status")),
                str(item.get("canonical_scientific_name") or item.get("sporely_taxon_id") or ""),
                label,
                str(item.get("current_revision") or ""),
                str(item.get("shared_at") or ""),
                str(item.get("withdrawn_at") or ""),
                QCoreApplication.translate("ReferenceSharing", "Hidden by moderation") if item.get("hidden_at") else "",
                _withdrawal_reason_label(item.get("withdrawal_reason")),
            )
            for column, value in enumerate(values):
                cell = QTableWidgetItem(value)
                cell.setData(Qt.UserRole, item.get("contribution_id"))
                self.table.setItem(index, column, cell)
        self.status_label.setText(
            QCoreApplication.translate("ReferenceSharing", "You have no shared references.") if not self._rows else ""
        )
        self._update_buttons()

    def _selected_row(self) -> dict | None:
        indexes = self.table.selectionModel().selectedRows() if self.table.selectionModel() else []
        if not indexes:
            return None
        index = indexes[0].row()
        return self._rows[index] if 0 <= index < len(self._rows) else None

    def _update_buttons(self) -> None:
        row = self._selected_row()
        self.stop_btn.setEnabled(bool(row and row.get("status") == "shared"))

    def confirm_stop_sharing(self) -> bool:
        answer = QMessageBox.question(
            self,
            QCoreApplication.translate("ReferenceSharing", "Stop sharing"),
            QCoreApplication.translate("ReferenceSharing", 
                "Stop sharing this reference publicly? It will no longer be "
                "shown to others.\n\nStopping cannot undo:\n"
                "- copies other users have already made,\n"
                "- earlier revisions Sporely keeps privately,\n"
                "- anything already cached by third parties."
            ),
            QMessageBox.Yes | QMessageBox.Cancel,
            QMessageBox.Cancel,
        )
        return answer == QMessageBox.Yes

    def _on_stop_clicked(self) -> None:
        row = self._selected_row()
        if not row or row.get("status") != "shared":
            return
        if not self.confirm_stop_sharing():
            return
        try:
            result = self._client.withdraw_reference_contribution(
                str(row.get("contribution_id"))
            )
        except Exception as exc:
            message = (
                share_status_message("rate_limited")[1]
                if _is_rate_limited_error(exc)
                else QCoreApplication.translate("ReferenceSharing", "Could not reach Sporely Cloud: {error}").format(error=str(exc))
            )
            QMessageBox.warning(self, self.windowTitle(), message)
            return
        status = result.get("status") if isinstance(result, dict) else None
        if status == "rate_limited":
            QMessageBox.warning(self, self.windowTitle(), share_status_message("rate_limited")[1])
        self.refresh()
