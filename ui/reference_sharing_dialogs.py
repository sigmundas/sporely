"""Owner UI for default-on public reference sharing.

Reference sets attached to a public, non-draft observation whose spore data
is public are shared by the server automatically; there is no consent step.
:class:`MySharedReferencesDialog` lists the owner's reference sets from
``list_my_reference_sharing`` (sporely-web
``20261001113007_share_references_by_default``) with a per-set status:

* ``shared``  -- shown publicly; offers "Stop sharing"
  (``stop_sharing_reference_set``).
* ``stopped`` -- the owner stopped it; offers "Share again"
  (``share_reference_set_again``).
* ``hidden``  -- hidden by moderation. The owner cannot override it, so no
  action is offered.

Every action reloads the list afterwards, so the status shown is always the
server's, never an optimistic guess.
"""
from __future__ import annotations

from PySide6.QtCore import QCoreApplication, Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

SET_STATUSES = ("shared", "stopped", "hidden")


def _is_rate_limited_error(exc: Exception) -> bool:
    return "rate_limited" in str(exc) or "429" in str(exc)


def _is_serialization_error(exc: Exception) -> bool:
    return "40001" in str(exc)


def rate_limited_message(result: dict | None = None) -> str:
    seconds = None
    if isinstance(result, dict):
        try:
            seconds = int(result.get("retry_after_seconds"))
        except (TypeError, ValueError):
            seconds = None
    if seconds and seconds > 0:
        return QCoreApplication.translate(
            "ReferenceSharing",
            "Too many requests. Please wait {seconds} seconds and try again.",
        ).format(seconds=seconds)
    return QCoreApplication.translate("ReferenceSharing", "Too many requests. Please wait a moment and try again.")


def _error_message(exc: Exception) -> str:
    if _is_rate_limited_error(exc):
        return rate_limited_message()
    if _is_serialization_error(exc):
        return QCoreApplication.translate("ReferenceSharing", "Sporely Cloud was busy with another change. Please try again.")
    return QCoreApplication.translate(
        "ReferenceSharing", "Could not reach Sporely Cloud: {error}").format(error=str(exc))


def show_message(parent, icon, title: str, text: str) -> None:
    """A plain-text message box (user/reference text is never rich text)."""
    box = QMessageBox(icon, title, text, QMessageBox.Ok, parent)
    box.setTextFormat(Qt.PlainText)
    box.exec()


def set_status(item: dict) -> str:
    """``shared``, ``stopped`` or ``hidden``. Hidden wins over stopped,
    matching the server; an unknown status is shown as-is, never as shared."""
    status = str(item.get("status") or "").strip().lower()
    if item.get("hidden_by_moderation") is True:
        return "hidden"
    return status


def status_label(status: str | None) -> str:
    return {
        "shared": QCoreApplication.translate("ReferenceSharing", "Shared"),
        "stopped": QCoreApplication.translate("ReferenceSharing", "Stopped"),
        "hidden": QCoreApplication.translate("ReferenceSharing", "Hidden"),
    }.get(str(status or ""), str(status or ""))


def set_action(status: str | None) -> str | None:
    """The one action offered for a set: ``stop``, ``share_again`` or None."""
    return {"shared": "stop", "stopped": "share_again"}.get(str(status or ""))


def action_result_message(action: str, result) -> tuple[bool, str | None]:
    """(success, message to show or None) for a stop/share-again result."""
    status = str(result.get("status") or "") if isinstance(result, dict) else ""
    if status in ("updated", "no_change"):
        return True, None
    if status == "rate_limited":
        return False, rate_limited_message(result)
    if status == "not_found":
        return False, QCoreApplication.translate("ReferenceSharing", "This reference set was not found in your account.")
    if action == "stop":
        return False, QCoreApplication.translate(
            "ReferenceSharing", "Could not stop sharing ({status}).").format(status=status or "?")
    return False, QCoreApplication.translate(
        "ReferenceSharing", "Could not share again ({status}).").format(status=status or "?")


def _set_title(item: dict) -> str:
    parts = [
        str(v).strip() for v in (item.get("source_short_label"), item.get("source_raw_text"))
        if isinstance(v, str) and v.strip()
    ]
    return " — ".join(parts) if parts else QCoreApplication.translate("ReferenceSharing", "(unnamed reference)")


def _species_text(item: dict) -> str:
    listings = item.get("species_page_contributions")
    if not isinstance(listings, list):
        return ""
    names = []
    for listing in listings:
        if not isinstance(listing, dict):
            continue
        name = str(listing.get("canonical_scientific_name") or listing.get("sporely_taxon_id") or "").strip()
        if name and name not in names:
            names.append(name)
    return ", ".join(names)


class MySharedReferencesDialog(QDialog):
    """The owner's reference sets with Stop sharing / Share again."""

    COLUMNS = ("status", "reference", "public_observations", "species", "stopped")

    def __init__(self, cloud_client, parent=None) -> None:
        super().__init__(parent)
        self._client = cloud_client
        self._rows: list[dict] = []
        self.setWindowTitle(QCoreApplication.translate("ReferenceSharing", "My shared references"))
        self.resize(900, 420)

        layout = QVBoxLayout(self)
        intro = QLabel(QCoreApplication.translate("ReferenceSharing", 
            "References attached to your public observations are shared "
            "automatically, with their relationship to the observation "
            "(compared, supports the identification or contradicts the "
            "identification). You can stop sharing a reference set here and "
            "share it again later. A set hidden by moderation cannot be shared "
            "again by you."), self)
        intro.setWordWrap(True)
        intro.setTextFormat(Qt.PlainText)
        layout.addWidget(intro)
        self.status_label = QLabel(self)
        self.status_label.setWordWrap(True)
        self.status_label.setTextFormat(Qt.PlainText)
        layout.addWidget(self.status_label)
        self.table = QTableWidget(0, len(self.COLUMNS), self)
        self.table.setHorizontalHeaderLabels([
            QCoreApplication.translate("ReferenceSharing", "Status"), QCoreApplication.translate("ReferenceSharing", "Reference"), QCoreApplication.translate("ReferenceSharing", "Public observations"),
            QCoreApplication.translate("ReferenceSharing", "Species-page listing"), QCoreApplication.translate("ReferenceSharing", "Stopped"),
        ])
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.itemSelectionChanged.connect(self._update_buttons)
        layout.addWidget(self.table, 1)
        self.hint_label = QLabel(self)
        self.hint_label.setWordWrap(True)
        self.hint_label.setTextFormat(Qt.PlainText)
        layout.addWidget(self.hint_label)

        row = QHBoxLayout()
        self.refresh_btn = QPushButton(QCoreApplication.translate("ReferenceSharing", "Refresh"), self)
        self.refresh_btn.clicked.connect(self.refresh)
        row.addWidget(self.refresh_btn)
        row.addStretch(1)
        self.stop_btn = QPushButton(QCoreApplication.translate("ReferenceSharing", "Stop sharing…"), self)
        self.stop_btn.clicked.connect(self._on_stop_clicked)
        row.addWidget(self.stop_btn)
        self.share_again_btn = QPushButton(QCoreApplication.translate("ReferenceSharing", "Share again"), self)
        self.share_again_btn.clicked.connect(self._on_share_again_clicked)
        row.addWidget(self.share_again_btn)
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
            result = self._client.list_my_reference_sharing()
        except Exception as exc:
            self.status_label.setText(_error_message(exc))
            self._update_buttons()
            return
        if isinstance(result, dict) and result.get("status") == "rate_limited":
            self.status_label.setText(rate_limited_message(result))
            self._update_buttons()
            return
        if (
            not isinstance(result, dict)
            or result.get("status") != "ok"
            or not isinstance(result.get("sets"), list)
        ):
            self.status_label.setText(QCoreApplication.translate("ReferenceSharing", "Could not load your shared references."))
            self._update_buttons()
            return
        self._rows = [r for r in result["sets"] if isinstance(r, dict)]
        self.table.setRowCount(len(self._rows))
        for index, item in enumerate(self._rows):
            try:
                count = int(item.get("public_observation_count") or 0)
            except (TypeError, ValueError):
                count = 0
            values = (
                status_label(set_status(item)),
                _set_title(item),
                str(count),
                _species_text(item),
                str(item.get("stopped_at") or ""),
            )
            for column, value in enumerate(values):
                cell = QTableWidgetItem(value)
                cell.setData(Qt.UserRole, item.get("source_measurement_set_id"))
                self.table.setItem(index, column, cell)
        self.status_label.setText(
            QCoreApplication.translate("ReferenceSharing", "You have no reference sets attached to public observations.")
            if not self._rows else ""
        )
        self._update_buttons()

    def _selected_row(self) -> dict | None:
        model = self.table.selectionModel()
        indexes = model.selectedRows() if model else []
        if not indexes:
            return None
        index = indexes[0].row()
        return self._rows[index] if 0 <= index < len(self._rows) else None

    def select_row(self, index: int) -> None:
        self.table.selectRow(index)
        self._update_buttons()

    def _update_buttons(self) -> None:
        row = self._selected_row()
        status = set_status(row) if row else None
        action = set_action(status)
        self.stop_btn.setEnabled(action == "stop")
        self.share_again_btn.setEnabled(action == "share_again")
        if status == "hidden":
            self.hint_label.setText(QCoreApplication.translate("ReferenceSharing", 
                "This reference set was hidden by moderation. It is not shown "
                "publicly, and you cannot share it again yourself."))
        else:
            self.hint_label.setText("")

    def confirm_stop_sharing(self) -> bool:
        box = QMessageBox(
            QMessageBox.Question,
            QCoreApplication.translate("ReferenceSharing", "Stop sharing"),
            QCoreApplication.translate("ReferenceSharing", 
                "Stop sharing this reference set? It stops being shown publicly "
                "everywhere in Sporely — on your observations, in plots and in "
                "the species-page listing — until you choose Share again.\n\n"
                "Stopping cannot recall copies other users have already made, "
                "or anything others have already downloaded, saved or cited. "
                "Sporely keeps earlier versions privately as a record."
            ),
            QMessageBox.Yes | QMessageBox.Cancel,
            self,
        )
        box.setTextFormat(Qt.PlainText)
        box.setDefaultButton(QMessageBox.Cancel)
        return box.exec() == QMessageBox.Yes

    def _run_action(self, action: str) -> None:
        row = self._selected_row()
        if not row or set_action(set_status(row)) != action:
            return
        set_id = str(row.get("source_measurement_set_id") or "")
        if not set_id:
            return
        if action == "stop" and not self.confirm_stop_sharing():
            return
        try:
            if action == "stop":
                result = self._client.stop_sharing_reference_set(set_id)
            else:
                result = self._client.share_reference_set_again(set_id)
        except Exception as exc:
            show_message(self, QMessageBox.Warning, self.windowTitle(), _error_message(exc))
            self.refresh()
            return
        ok, message = action_result_message(action, result)
        self.refresh()
        if ok and action == "share_again":
            # Report what the server now says, never the request alone.
            fresh = next(
                (r for r in self._rows if str(r.get("source_measurement_set_id")) == set_id),
                None,
            )
            status = set_status(fresh) if fresh else None
            if status == "hidden":
                ok, message = False, QCoreApplication.translate("ReferenceSharing", 
                    "This reference set is hidden by moderation and is not "
                    "shown publicly.")
            elif fresh is None:
                # The server accepted (updated / no_change) but lists the set
                # nowhere yet: no public observation uses it right now.
                message = QCoreApplication.translate(
                    "ReferenceSharing",
                    "Sharing is restored for this reference set. It will be shown "
                    "when it is attached to a public observation.")
            elif status != "shared":
                ok, message = False, QCoreApplication.translate("ReferenceSharing", 
                    "Sporely couldn't confirm that this reference set is shared "
                    "again. Refresh to check its status.")
        if message:
            icon = QMessageBox.Information if ok else QMessageBox.Warning
            show_message(self, icon, self.windowTitle(), message)

    def _on_stop_clicked(self) -> None:
        self._run_action("stop")

    def _on_share_again_clicked(self) -> None:
        self._run_action("share_again")
