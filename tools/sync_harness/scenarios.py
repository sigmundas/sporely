"""Sporely-owned desktop GUI scenarios for the disposable-target sync harness.

The external harness (``g4-evidence/tools``) loads this file from the admitted
worktree when a scenario is named ``repo:<function>``. Each function takes the
harness driver context (``g4_driver.Ctx``) and drives the real ``MainWindow``
with widget interactions; cloud and database effects come from the app's own
handlers. The harness's own scenarios (``g4_scenarios``) and helpers are reused.

Two kinds of controlled substitution are used, and both are recorded in the
run's ``steps.jsonl``:

* Native file dialogs cannot be driven off-screen. Where the user would pick a
  file, ``QFileDialog``'s static getter returns the path from the scenario
  arguments (the dialog the user sees is the only thing replaced).
* Fault injection (``sync_with_fault``) wraps ``requests.Session.request`` that
  the harness already installed, to stop the process (``os._exit``) or raise a
  timeout/connection error just before or after a matching request. It never
  sends requests of its own and never alters a request or response.

No credentials, hosts or network code live here.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit

from PySide6.QtCore import QPoint, QPointF, Qt, QTimer
from PySide6.QtTest import QTest
from PySide6.QtWidgets import QApplication, QDialog, QFileDialog, QMessageBox

import g4_scenarios as g4


# ---------------------------------------------------------------------------
# Local database reads (read-only; used to resolve ids and to log state)
# ---------------------------------------------------------------------------
def _db_path() -> Path:
    return Path(os.environ["SPORELY_APP_DATA_DIR"]) / "mushrooms.db"


def _query(sql: str, params: tuple = ()) -> list[dict]:
    con = sqlite3.connect(f"file:{_db_path()}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(sql, params)]
    finally:
        con.close()


def _obs_local_id(ctx, spec) -> int:
    """spec: {"local_id": n} | {"cloud_id": n} | {"newest": true}"""
    if spec.get("local_id") is not None:
        return int(spec["local_id"])
    if spec.get("cloud_id") is not None:
        return g4._local_id_for_cloud(ctx, spec["cloud_id"])
    if spec.get("newest"):
        return int(_query("select max(id) as id from observations")[0]["id"])
    raise RuntimeError(f"cannot resolve observation from {spec}")


def _log_local_state(ctx, label: str) -> None:
    def rows(table, cols):
        have = {r["name"] for r in _query(f"pragma table_info({table})")}
        use = [c for c in cols if c in have]
        return _query(f"select {', '.join(use)} from {table} order by id")
    state = {
        "observations": rows("observations", ["id", "cloud_id", "sync_key", "legacy_recovery_pending", "sync_status"]),
        "images": rows("images", ["id", "observation_id", "cloud_id", "sync_key", "legacy_recovery_pending"]),
        "measurements": rows("spore_measurements", ["id", "image_id", "cloud_id", "sync_key", "legacy_recovery_pending"]),
    }
    path = ctx.run_dir / f"local-{label}.json"
    path.write_text(json.dumps(state, indent=1, default=str))
    ctx.log("local_state", label=label, path=str(path),
            counts={k: len(v) for k, v in state.items()})


def _click_main_tab(ctx, index: int) -> None:
    bar = ctx.w.tab_widget.tabBar()
    QTest.mouseClick(bar, Qt.LeftButton, Qt.NoModifier, bar.tabRect(index).center())
    ctx.wait(800)
    ctx.log("main_tab", index=index, current=ctx.w.tab_widget.currentIndex(), text=bar.tabText(index))
    if ctx.w.tab_widget.currentIndex() != index:
        raise RuntimeError(f"tab {index} not activated")


def _file_dialog_returns(ctx, getter: str, path: str):
    """Make QFileDialog.<getter> return ``path`` once (the user's file choice)."""
    real = getattr(QFileDialog, getter)

    def chosen(*_a, **_kw):
        setattr(QFileDialog, getter, real)
        ctx.log("file_dialog", getter=getter, chosen=path)
        return path, ""
    setattr(QFileDialog, getter, staticmethod(chosen))


def _record_sync_errors(ctx) -> None:
    """Observation only: record the message of a failed sync, then call through."""
    tab = ctx.w.observations_tab
    if getattr(tab, "_s6_error_recorder", False):
        return
    real = tab._on_cloud_sync_error

    def recording(message, *a, **kw):
        ctx.log("sync_error", message=str(message)[:4000])
        ctx.result.setdefault("sync_errors", []).append(str(message)[:4000])
        return real(message, *a, **kw)

    tab._on_cloud_sync_error = recording
    tab._s6_error_recorder = True


def _sync_now(ctx) -> None:
    _record_sync_errors(ctx)
    g4.sync_now(ctx)


# ---------------------------------------------------------------------------
# State probe and restart
# ---------------------------------------------------------------------------
def probe(ctx):
    """Start, record local identity state and a screenshot, quit."""
    ctx.wait(2000)
    _log_local_state(ctx, "probe")
    ctx.result["table_rows"] = ctx.w.observations_tab.table.rowCount()
    ctx.shot("probe")


# ---------------------------------------------------------------------------
# Sync with an injected fault
# ---------------------------------------------------------------------------
def _install_fault(ctx, specs: list[dict]) -> None:
    import requests

    inner = requests.Session.request
    lock = threading.Lock()
    counters = [0 for _ in specs]

    def matches(spec, method, url):
        if str(method).upper() != spec.get("method", "POST").upper():
            return False
        path = urlsplit(str(url)).path
        if spec.get("path") is not None:
            return path == spec["path"]
        return spec.get("path_contains", "\0") in path

    def die(spec, phase, method, url, status=None):
        rec = {"status": "KILLED", "fault": spec, "phase": phase, "method": str(method).upper(),
               "path": urlsplit(str(url)).path, "response_status": status, "t": time.time()}
        ctx.log("fault_kill", **rec)
        (ctx.run_dir / "fault.json").write_text(json.dumps(rec, indent=1, default=str))
        (ctx.run_dir / "result.json").write_text(json.dumps({**ctx.result, **rec}, indent=1, default=str))
        os._exit(137)

    def faulty(self, method, url, *a, **kw):
        if str(method).upper() not in ("GET", "HEAD", "OPTIONS"):
            # Observation only: the identity headers this write carries.
            merged = {str(k).lower(): v for k, v in dict(self.headers or {}).items()}
            merged.update({str(k).lower(): v for k, v in dict(kw.get("headers") or {}).items()})
            with (ctx.run_dir / "write_headers.jsonl").open("a") as fh:
                fh.write(json.dumps({"method": str(method).upper(), "path": urlsplit(str(url)).path,
                                     "x-sporely-sync-identity": merged.get("x-sporely-sync-identity"),
                                     "x-sporely-client-version": merged.get("x-sporely-client-version")}) + "\n")
        hit = None
        with lock:
            for i, spec in enumerate(specs):
                if matches(spec, method, url):
                    counters[i] += 1
                    nth, times = int(spec.get("nth", 1)), int(spec.get("times", 1))
                    if nth <= counters[i] < nth + times:
                        hit = spec
                        break
        if hit is None:
            return inner(self, method, url, *a, **kw)
        kind = hit["kind"]
        if kind == "kill_before":
            die(hit, "before_request", method, url)
        if kind == "conn_error":
            ctx.log("fault_conn_error", method=str(method).upper(), path=urlsplit(str(url)).path)
            ctx.result.setdefault("faults", []).append({"kind": kind, "path": urlsplit(str(url)).path})
            raise requests.exceptions.ConnectionError("harness fault: connection error before request")
        resp = inner(self, method, url, *a, **kw)
        if kind == "kill_after":
            die(hit, "after_response", method, url, resp.status_code)
        if kind == "timeout_after":
            ctx.log("fault_timeout_after", method=str(method).upper(), path=urlsplit(str(url)).path,
                    response_status=resp.status_code)
            ctx.result.setdefault("faults", []).append({"kind": kind, "path": urlsplit(str(url)).path,
                                                        "discarded_status": resp.status_code})
            raise requests.exceptions.ReadTimeout("harness fault: read timeout after the server answered")
        return resp

    requests.Session.request = faulty
    ctx.log("fault_installed", specs=specs)


def sync_with_fault(ctx):
    """args: faults [{kind: kill_before|kill_after|timeout_after|conn_error,
    method, path | path_contains, nth, times}]. Records local state, installs
    the faults, clicks Sync now."""
    ctx.wait(1000)
    _log_local_state(ctx, "before")
    _install_fault(ctx, list(ctx.args.get("faults") or []))
    _sync_now(ctx)
    _log_local_state(ctx, "after")


def sync_and_record(ctx):
    """Sync now, recording local identity state before and after and the
    identity headers of every write request."""
    ctx.wait(1000)
    _log_local_state(ctx, "before")
    _install_fault(ctx, [])
    _sync_now(ctx)
    _log_local_state(ctx, "after")


# ---------------------------------------------------------------------------
# New observation from a microscope image (scale from the objective)
# ---------------------------------------------------------------------------
def _drop_files(ctx, files):
    from PySide6.QtCore import QMimeData, QUrl
    from PySide6.QtGui import QDragEnterEvent, QDragMoveEvent, QDropEvent
    tab = ctx.w.observations_tab
    mime = QMimeData()
    mime.setUrls([QUrl.fromLocalFile(str(Path(p).resolve())) for p in files])
    pos = QPointF(tab.width() / 2, tab.height() / 2)
    enter = QDragEnterEvent(pos.toPoint(), Qt.CopyAction, mime, Qt.LeftButton, Qt.NoModifier)
    QApplication.sendEvent(tab, enter)
    QApplication.sendEvent(tab, QDragMoveEvent(pos.toPoint(), Qt.CopyAction, mime, Qt.LeftButton, Qt.NoModifier))
    ctx.log("drop", files=list(files), enter_accepted=enter.isAccepted())
    QApplication.sendEvent(tab, QDropEvent(pos, Qt.CopyAction, mime, Qt.LeftButton, Qt.NoModifier))


def _choose_objective(ctx, selector, needle: str) -> None:
    """Select the objective whose label contains ``needle`` with a pill click or
    the combo's keyboard, whichever the selector currently shows."""
    current = selector.itemText(selector.currentIndex())
    ctx.log("objective_current", text=current, data=str(selector.currentData()))
    if needle in current:
        return
    pills = [b for b in selector.buttons() if b.isVisible() and needle in (b.toolTip() or b.text())]
    if pills:
        ctx.click(pills[0], f"objective {pills[0].text()}")
        return
    combo = selector._combo
    combo.setFocus()
    for _ in range(combo.count()):
        if needle in combo.currentText():
            break
        QTest.keyClick(combo, Qt.Key_Down)
        ctx.wait(200)
    ctx.log("objective_combo", text=combo.currentText())
    if needle not in selector.itemText(selector.currentIndex()):
        raise RuntimeError(f"objective {needle!r} not selectable")


def _guarded(ctx, action):
    """Run a modal handler; on failure close the dialog so the run ends
    instead of waiting for the watchdog."""
    def run(dlg):
        try:
            action(dlg)
        except Exception:
            ctx.shot("handler-failed", dlg)
            dlg.reject()
            raise
    return run


def new_microscope_observation(ctx):
    """Drop image files on the list, mark them Microscope with an objective in
    Prepare Images, Continue, then Create Observation.
    args: files, objective (substring, default "100X"), sync"""
    from ui.image_import_dialog import ImageImportDialog
    from ui.observations_tab import ObservationDetailsDialog
    a = ctx.args
    ctx.wait(1500)

    def prepare(dlg):
        ctx.wait(2500)
        ctx.click(dlg.micro_radio, "Microscope Image")
        ctx.wait(600)
        _choose_objective(ctx, dlg.objective_combo, a.get("objective", "100X"))
        ctx.wait(600)
        ctx.result["pi_micro"] = {"objective": str(dlg.objective_combo.currentData()),
                                  "micro": dlg.micro_radio.isChecked()}
        ctx.shot("prepare-images-micro", dlg)
        ctx.click(dlg.next_btn, "Prepare Images continue")

    def details(dlg):
        ctx.wait(1500)
        ctx.shot("details-new-micro", dlg)
        ctx.click(dlg.submit_observation_btn, "Create Observation")

    ctx.on_modal(lambda m: isinstance(m, ImageImportDialog), _guarded(ctx, prepare), "prepare-images-micro")
    ctx.on_modal(lambda m: isinstance(m, ObservationDetailsDialog), _guarded(ctx, details), "details-new-micro")
    _drop_files(ctx, a["files"])
    ctx.wait(4000)
    pending = [r["name"] for r in ctx.modal_rules]
    if pending:
        raise RuntimeError(f"dialogs not reached: {pending}")
    ctx.wait(2000)
    newest = _query("select id from observations order by id desc limit 1")[0]["id"]
    ctx.result["new_observation"] = newest
    ctx.result["new_images"] = _query(
        "select id, image_type, scale_microns_per_pixel, sync_key from images where observation_id=?", (newest,))
    _log_local_state(ctx, "after-create")
    if a.get("sync"):
        _sync_now(ctx)
        _log_local_state(ctx, "after-sync")


def new_observation_without_images(ctx):
    """Click "+ New", continue from an empty Prepare Images, then Create
    Observation. args: sync"""
    from ui.image_import_dialog import ImageImportDialog
    from ui.observations_tab import ObservationDetailsDialog
    a = ctx.args
    ctx.wait(1500)
    before = {r["id"] for r in _query("select id from observations")}

    def prepare(dlg):
        ctx.wait(1500)
        ctx.result["pi_images"] = len(getattr(dlg, "import_results", []) or [])
        ctx.shot("prepare-images-empty", dlg)
        ctx.click(dlg.next_btn, "Prepare Images continue")

    def details(dlg):
        ctx.wait(1500)
        ctx.shot("details-new-no-images", dlg)
        ctx.click(dlg.submit_observation_btn, "Create Observation")

    ctx.on_modal(lambda m: isinstance(m, ImageImportDialog), _guarded(ctx, prepare), "prepare-images-empty")
    ctx.on_modal(lambda m: isinstance(m, ObservationDetailsDialog), _guarded(ctx, details), "details-new-no-images")
    ctx.click(ctx.w.observations_tab.new_btn, "+ New")
    ctx.wait(4000)
    pending = [r["name"] for r in ctx.modal_rules]
    if pending:
        raise RuntimeError(f"dialogs not reached: {pending}")
    ctx.wait(1500)
    created = sorted({r["id"] for r in _query("select id from observations")} - before)
    if len(created) != 1:
        raise RuntimeError(f"expected one new observation, got {created}")
    ctx.result["new_observation"] = created[0]
    ctx.result["new_images"] = _query("select id from images where observation_id=?", (created[0],))
    _log_local_state(ctx, "after-create")
    if a.get("sync"):
        _sync_now(ctx)
        _log_local_state(ctx, "after-sync")


# ---------------------------------------------------------------------------
# Spore measurement creation in the Measure tab
# ---------------------------------------------------------------------------
def measure_new(ctx):
    """Select an observation, open the Measure tab, pick a scale if the image
    has none, start measuring and draw rectangles with mouse clicks.
    args: obs {local_id|cloud_id|newest}, rects (default 1), scale_index, sync"""
    a = ctx.args
    w = ctx.w
    ctx.wait(1500)
    local_id = _obs_local_id(ctx, a.get("obs") or {"newest": True})
    g4._select_observation(ctx, local_id)
    _click_main_tab(ctx, 1)
    ctx.wait_until(lambda: getattr(w, "current_image_id", None), 30, "measure image")
    ctx.wait(1500)
    combo = w.scale_combo
    info = {"image_id": w.current_image_id, "mpp": w.microns_per_pixel,
            "scale_items": [combo.itemText(i) for i in range(combo.count())], "scale_index": combo.currentIndex()}
    ctx.log("measure_opened", **info)
    ctx.result["measure_opened"] = info
    ctx.shot("measure-opened")
    if not (w.microns_per_pixel and w.microns_per_pixel > 0):
        target = int(a.get("scale_index", 2))
        combo.setFocus()
        ctx.wait(200)
        while combo.currentIndex() < target:
            QTest.keyClick(combo, Qt.Key_Down)
            ctx.wait(400)
        ctx.log("scale_chosen", index=combo.currentIndex(), text=combo.currentText(), mpp=w.microns_per_pixel)
    ctx.log("measure_state", active=w.measurement_active, button=w.measure_button.text())
    if not w.measurement_active:
        ctx.click(w.measure_button, "Start measuring")
        ctx.wait(800)
    if not w.measurement_active:
        raise RuntimeError(f"measuring not active: {w.measure_status_label.text()}")
    label = w.image_label
    before = len(_query("select id from spore_measurements where image_id=?", (int(w.current_image_id),)))
    cx, cy = label.width() // 2, label.height() // 2
    for r in range(int(a.get("rects", 1))):
        ox = (r - 1) * 40
        # rectangle mode: two clicks along the length, two along the width
        for dx, dy in ((-30, 0), (30, 0), (0, -12), (0, 12)):
            QTest.mouseClick(label, Qt.LeftButton, Qt.NoModifier, QPoint(cx + ox + dx, cy + dy))
            ctx.wait(300)
        ctx.wait(600)
    after = len(_query("select id from spore_measurements where image_id=?", (int(w.current_image_id),)))
    ctx.result["measurements_added"] = after - before
    ctx.log("measured", image_id=w.current_image_id, before=before, after=after,
            status=w.measure_status_label.text())
    ctx.shot("measured")
    if after <= before:
        raise RuntimeError("no measurement row was created")
    ctx.click(w.measure_button, "Stop measuring")
    ctx.wait(500)
    _click_main_tab(ctx, 0)
    _log_local_state(ctx, "after-measure")
    if a.get("sync"):
        _sync_now(ctx)
        _log_local_state(ctx, "after-sync")


# ---------------------------------------------------------------------------
# Portable export / import and legacy bundle import
# ---------------------------------------------------------------------------
def export_portable(ctx):
    """Select observations in the table and export them with File > Export
    Selected Observations. args: obs [{local_id|cloud_id}], path"""
    a = ctx.args
    w = ctx.w
    ctx.wait(1500)
    tab = w.observations_tab
    ids = [_obs_local_id(ctx, s) for s in a["obs"]]
    g4._select_observation(ctx, ids[0])
    for extra in ids[1:]:
        for r in range(tab.table.rowCount()):
            if tab._observation_id_for_row(r) == extra:
                rect = tab.table.visualItemRect(tab.table.item(r, 1) or tab.table.item(r, 0))
                QTest.mouseClick(tab.table.viewport(), Qt.LeftButton, Qt.ControlModifier, rect.center())
                ctx.wait(300)
    ctx.log("selected_for_export", ids=sorted(tab.selected_observation_ids()))
    ctx.on_modal(lambda m: isinstance(m, QMessageBox), g4._ok_box(ctx, "export_box"), "export-box", once=False)
    _file_dialog_returns(ctx, "getSaveFileName", a["path"])
    w.export_selected_observations_action.trigger()
    ctx.wait_until(lambda: Path(a["path"]).exists() and (w._portable_export_worker is None
                                                          or not w._portable_export_worker.isRunning()),
                   120, "portable export")
    ctx.wait(1500)
    ctx.result["exported"] = {"path": a["path"], "bytes": Path(a["path"]).stat().st_size}
    ctx.shot("exported")


def import_archive(ctx):
    """File > Import Observations with ``path``. A portable archive goes through
    the import preview dialog (Import); a legacy package through the bundle
    options dialog (OK with its defaults). args: path, sync"""
    from ui.portable_import_dialog import PortableImportDialog
    a = ctx.args
    w = ctx.w
    ctx.wait(1500)
    _log_local_state(ctx, "before-import")
    before = len(_query("select id from observations"))

    def portable(dlg):
        ctx.wait(1000)
        ctx.shot("portable-import-dialog", dlg)
        ctx.click(dlg.import_button, "Import")

    def bundle(dlg):
        ctx.wait(800)
        # Tick or untick option boxes as a user would (args: bundle_options
        # {"references_check": false, ...}); the rest keep their defaults.
        for attr, wanted in (a.get("bundle_options") or {}).items():
            box = getattr(dlg, attr)
            if box.isChecked() != bool(wanted):
                ctx.click(box, f"bundle option {attr}")
                ctx.wait(200)
        opts = dlg.get_options() if hasattr(dlg, "get_options") else None
        ctx.log("bundle_options", options=opts)
        ctx.result["bundle_options"] = opts
        ctx.shot("bundle-options", dlg)
        dlg.accept()

    def is_bundle(m):
        return isinstance(m, QDialog) and type(m).__name__ == "DatabaseBundleOptionsDialog"
    ctx.on_modal(lambda m: isinstance(m, PortableImportDialog), _guarded(ctx, portable), "portable-import")
    ctx.on_modal(is_bundle, _guarded(ctx, bundle), "bundle-options")
    ctx.on_modal(lambda m: isinstance(m, QMessageBox), g4._ok_box(ctx, "import_box"), "import-box", once=False)
    _file_dialog_returns(ctx, "getOpenFileName", a["path"])
    w.import_observations_action.trigger()
    ctx.wait_until(lambda: len(_query("select id from observations")) > before
                   and getattr(w, "_portable_import_worker", None) is None, 180, "import to finish")
    ctx.wait(2500)
    ctx.result["imported_observations"] = len(_query("select id from observations")) - before
    _log_local_state(ctx, "after-import")
    ctx.shot("imported")
    if a.get("sync"):
        _sync_now(ctx)
        _log_local_state(ctx, "after-sync")


# ---------------------------------------------------------------------------
# Image move (gallery context menu "Move to observation", then target row)
# ---------------------------------------------------------------------------
def _choose_popup_action(ctx, text: str):
    def pick():
        menu = QApplication.activePopupWidget()
        if menu is None:
            QTimer.singleShot(150, pick)
            return
        acts = [act for act in menu.actions() if act.text().replace("&", "") == text]
        ctx.log("context_menu", actions=[act.text() for act in menu.actions()], choosing=text)
        if not acts:
            menu.close()
            return
        menu.setActiveAction(acts[0])
        QTest.keyClick(menu, Qt.Key_Return)
    QTimer.singleShot(300, pick)


def move_image(ctx):
    """Move one image to another observation through the gallery context menu.
    args: source {local_id|cloud_id}, image_local_id (default: first image of
    the source), target {local_id|cloud_id}, sync"""
    a = ctx.args
    tab = ctx.w.observations_tab
    ctx.wait(1500)
    src = _obs_local_id(ctx, a["source"])
    dst = _obs_local_id(ctx, a["target"])
    image_id = a.get("image_local_id") or _query(
        "select id from images where observation_id=? order by id limit 1", (src,))[0]["id"]
    _log_local_state(ctx, "before-move")
    g4._select_observation(ctx, src)
    gallery = tab.gallery_widget
    ctx.wait_until(lambda: any(getattr(f, "image_id", None) == image_id for f in gallery._frames), 30, "gallery frame")
    frame = next(f for f in gallery._frames if getattr(f, "image_id", None) == image_id)
    gallery.ensureWidgetVisible(frame) if hasattr(gallery, "ensureWidgetVisible") else None
    ctx.wait(300)
    _choose_popup_action(ctx, "Move to observation")
    center = QPoint(frame.width() // 2, frame.height() // 2)
    QTest.mouseClick(frame, Qt.RightButton, Qt.NoModifier, center)
    ctx.wait(1200)
    if not tab._pending_gallery_move_image_ids:
        raise RuntimeError("move mode not entered")
    ctx.log("move_pending", image_ids=list(tab._pending_gallery_move_image_ids), source=src)
    for r in range(tab.table.rowCount()):
        if tab._observation_id_for_row(r) == dst:
            tab.table.scrollToItem(tab.table.item(r, 1) or tab.table.item(r, 0))
            rect = tab.table.visualItemRect(tab.table.item(r, 1) or tab.table.item(r, 0))
            QTest.mouseClick(tab.table.viewport(), Qt.LeftButton, Qt.NoModifier, rect.center())
            break
    else:
        raise RuntimeError(f"target observation {dst} not in table")
    ctx.wait(2500)
    moved = _query("select id, observation_id, cloud_id, sync_key from images where observation_id=? order by id", (dst,))
    ctx.result["target_images"] = moved
    ctx.log("moved", target=dst, target_images=moved)
    _log_local_state(ctx, "after-move")
    ctx.shot("moved")
    if a.get("sync"):
        _sync_now(ctx)
        _log_local_state(ctx, "after-sync")
