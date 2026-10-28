"""Drive the observation editor's taxon picker with real macOS input.

Offscreen Qt does not reproduce macOS focus and activation, so this check
runs the real `ObservationDetailsDialog` on the cocoa platform, isolated by
the review renderer's fixtures (temporary database, no settings, no cloud).
All input stays in THIS process: keys go through CoreGraphics
``CGEventPostToPid``; clicks and wheel events are native NSEvents queued in
this application's own event queue, because mouse events posted with
``CGEventPostToPid`` never reach a Qt window. It never uses System Events
keystrokes or global mouse events, which land in whatever app is frontmost.
Activation is logged at every step; a run whose window never became active
(a locked screen, say) is reported as environmental and exits 2.

The window comes to the front for about half a minute. From the worktree:

    QT_QPA_PLATFORM=cocoa .venv/bin/python -m tools.review_ui.taxon_picker_mac_check out.json [shot.png]

With a second path it also saves the dialog and the floating list, as Qt
renders them, at their real screen positions.

These are agent checks with synthetic events, not the owner's hands-on
verification.
"""
from __future__ import annotations

import ctypes
import ctypes.util
import json
import os
import subprocess
import sys
import time

from PySide6.QtCore import QEventLoop, QPoint, Qt, QTimer
from PySide6.QtWidgets import QApplication, QLabel

app = QApplication.instance() or QApplication([])

cg = ctypes.cdll.LoadLibrary(ctypes.util.find_library("ApplicationServices"))
cf = ctypes.cdll.LoadLibrary(ctypes.util.find_library("CoreFoundation"))
cg.CGEventCreateKeyboardEvent.restype = ctypes.c_void_p
cg.CGEventCreateKeyboardEvent.argtypes = [ctypes.c_void_p, ctypes.c_uint16, ctypes.c_bool]
cg.CGEventKeyboardSetUnicodeString.argtypes = [ctypes.c_void_p, ctypes.c_ulong, ctypes.POINTER(ctypes.c_uint16)]
cg.CGEventSetFlags.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
cg.CGEventPostToPid.argtypes = [ctypes.c_int, ctypes.c_void_p]


class CGPoint(ctypes.Structure):
    _fields_ = [("x", ctypes.c_double), ("y", ctypes.c_double)]


cg.CGEventCreateScrollWheelEvent.restype = ctypes.c_void_p
cg.CGEventCreateScrollWheelEvent.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_int32]
cf.CFRelease.argtypes = [ctypes.c_void_p]

PID = os.getpid()
KEY = {"return": 36, "tab": 48, "escape": 53, "left": 123, "right": 124, "down": 125, "up": 126,
       "pagedown": 121, "a": 0, "backspace": 51}
FLAG_ALT = 0x00080000
FLAG_CMD = 0x00100000


def post(event):
    cg.CGEventPostToPid(PID, event)
    cf.CFRelease(event)


def key(name: str, flags: int = 0):
    for down in (True, False):
        e = cg.CGEventCreateKeyboardEvent(None, KEY[name], down)
        if flags:
            cg.CGEventSetFlags(e, flags)
        post(e)
    pump(60)


def text(s: str):
    for ch in s:
        units = (ctypes.c_uint16 * 2)(*[ord(c) for c in ch.encode("utf-16-le").decode("utf-16-le")], 0)
        for down in (True, False):
            e = cg.CGEventCreateKeyboardEvent(None, 0, down)
            cg.CGEventKeyboardSetUnicodeString(e, 1, units)
            post(e)
        pump(25)


_objc = ctypes.cdll.LoadLibrary(ctypes.util.find_library("objc"))
_objc.objc_getClass.restype = ctypes.c_void_p
_objc.sel_registerName.restype = ctypes.c_void_p
cg.CGEventSetIntegerValueField.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_int64]
cg.CGEventSetLocation.argtypes = [ctypes.c_void_p, CGPoint]


def _msg(restype, receiver, selector: bytes, *args, argtypes=()):
    send = _objc.objc_msgSend
    send.restype = restype
    send.argtypes = [ctypes.c_void_p, ctypes.c_void_p, *argtypes]
    return send(receiver, _objc.sel_registerName(selector), *args)


def _nsapp():
    return _msg(ctypes.c_void_p, _objc.objc_getClass(b"NSApplication"), b"sharedApplication")


def _ns_window(widget):
    """The NSWindow and its number for the Qt window that holds ``widget``."""
    view = ctypes.c_void_p(int(widget.window().winId()))
    window = _msg(ctypes.c_void_p, view, b"window")
    return window, _msg(ctypes.c_long, window, b"windowNumber")


def _post_ns(event) -> None:
    """Queue a native event in THIS application's own Cocoa event queue.
    Mouse events sent with CGEventPostToPid never reach a Qt window; an
    NSEvent posted to NSApp goes through Cocoa's normal dispatch to the
    window it names."""
    _msg(None, _nsapp(), b"postEvent:atStart:", event, False, argtypes=(ctypes.c_void_p, ctypes.c_bool))


def click(widget, pos: QPoint | None = None):
    pump(150)  # let a layout that just changed settle before reading positions
    pos = pos if pos is not None else widget.rect().center()
    top = widget.window()
    local = widget.mapTo(top, pos)
    _window, number = _ns_window(widget)
    point = CGPoint(local.x(), top.height() - local.y())  # window coordinates, from the bottom left
    for kind in (1, 2):  # left mouse down, up
        event = _msg(
            ctypes.c_void_p, _objc.objc_getClass(b"NSEvent"),
            b"mouseEventWithType:location:modifierFlags:timestamp:windowNumber:context:eventNumber:clickCount:pressure:",
            kind, point, 0, time.monotonic(), number, None, 0, 1, ctypes.c_float(1.0 if kind == 1 else 0.0),
            argtypes=(ctypes.c_ulong, CGPoint, ctypes.c_ulong, ctypes.c_double, ctypes.c_long,
                      ctypes.c_void_p, ctypes.c_long, ctypes.c_long, ctypes.c_float),
        )
        _post_ns(event)
        pump(60)
    pump(120)


def _wheel(widget, units: int, amount: int):
    """A native scroll-wheel NSEvent handed to the widget's own NSWindow.
    AppKit has no public constructor for a scroll event that names a window,
    and a windowless one makes Qt read the real cursor position, so the
    CoreGraphics event is made window-relative with AppKit's private
    ``_eventRelativeToWindow:`` (test tooling only)."""
    g = widget.mapToGlobal(widget.rect().center())
    window, _number = _ns_window(widget)
    event = cg.CGEventCreateScrollWheelEvent(None, units, 1, amount)
    cg.CGEventSetLocation(event, CGPoint(g.x(), g.y()))
    ns_event = _msg(ctypes.c_void_p, _objc.objc_getClass(b"NSEvent"), b"eventWithCGEvent:", event,
                    argtypes=(ctypes.c_void_p,))
    relative = _msg(ctypes.c_void_p, ns_event, b"_eventRelativeToWindow:", window, argtypes=(ctypes.c_void_p,))
    _msg(None, window, b"sendEvent:", relative, argtypes=(ctypes.c_void_p,))
    cf.CFRelease(event)


def scroll(widget, lines: int):
    _wheel(widget, 1, lines)  # 1 = line units, a mouse wheel
    pump(200)


def glide(widget, pixels: int, steps: int = 8):
    """A trackpad-style scroll: several small pixel-unit wheel events."""
    for _ in range(steps):
        _wheel(widget, 0, pixels // steps)  # 0 = pixel units
        pump(16)
    pump(200)


def save_composite(dialog, popup, path: str) -> None:
    """The dialog and the floating list, each rendered by Qt at its real
    screen position. Qt's own renders, so no screen-recording permission is
    involved and nothing else on the screen is captured."""
    from PySide6.QtCore import QRect
    from PySide6.QtGui import QColor, QPainter, QPixmap

    parts = [(dialog.grab(), dialog.mapToGlobal(QPoint(0, 0)))]
    if popup.isVisible() and popup.isWindow():
        parts.append((popup.grab(), popup.mapToGlobal(QPoint(0, 0))))
    bounds = QRect()
    for pixmap, origin in parts:
        bounds = bounds.united(QRect(origin, pixmap.deviceIndependentSize().toSize()))
    ratio = parts[0][0].devicePixelRatio()
    canvas = QPixmap(bounds.size() * ratio)
    canvas.setDevicePixelRatio(ratio)
    canvas.fill(QColor("#8a8f8d"))
    painter = QPainter(canvas)
    for pixmap, origin in parts:
        painter.drawPixmap(origin - bounds.topLeft(), pixmap)
    painter.end()
    canvas.save(path, "PNG")


def pump(ms: int):
    """Run Qt's event loop for ``ms``. A nested QEventLoop, not a
    processEvents() loop: on Cocoa a modal dialog opened while a
    processEvents() call handles a click never services its own events."""
    loop = QEventLoop()
    QTimer.singleShot(ms, loop.quit)
    loop.exec()


def _activate_self():
    """[NSApp activateIgnoringOtherApps:YES] for this process only."""
    _msg(None, _nsapp(), b"activateIgnoringOtherApps:", True, argtypes=(ctypes.c_bool,))


def front(window=None):
    for attempt in range(6):
        r = subprocess.run(["osascript", "-e",
                            f'tell application "System Events" to set frontmost of (first process whose unix id is {PID}) to true'],
                           capture_output=True, text=True)
        _activate_self()
        if window is not None:
            window.raise_()
            window.activateWindow()
        pump(500)
        print("front attempt", attempt, "rc", r.returncode, r.stderr.strip()[:120],
              "active=", type(QApplication.activeWindow()).__name__, flush=True)
        if QApplication.activeWindow() is not None:
            return True
    return False


results: list[dict] = []


def record(step: str, ok: bool, **info):
    active = QApplication.activeWindow()
    entry = {"step": step, "ok": bool(ok), "active": type(active).__name__ if active else None, **info}
    results.append(entry)
    print(("PASS " if ok else "FAIL ") + step, json.dumps(info, ensure_ascii=False, default=str), "active=", entry["active"], flush=True)
    if active is None and _windows and _windows[0].isVisible():
        # Something else took activation (the user, say). Mark it and take it
        # back, so later steps are not all lost; this step's result is
        # environmental if it failed.
        entry["environmental"] = not ok
        front(_windows[0])


_windows: list = []


def main(out_path: str):
    from tools.review_ui.context import ReviewContext
    from tools.review_ui.scenarios import observations as sc

    with ReviewContext(app) as ctx:
        dialog = sc._observation_editor(ctx, draft={}, with_ai=True)
        accepted = []
        dialog.accepted.connect(lambda: accepted.append(True))
        # Short and at the top of the screen, so the list has to reach past
        # the dialog's bottom edge to open downwards.
        area = app.primaryScreen().availableGeometry()
        dialog.resize(1200, min(760, area.height() - 120))
        dialog.move(area.x() + 40, area.y() + 10)
        dialog.show()
        _windows.append(dialog)
        sc._settle_editor_layout(dialog)
        if not front(dialog):
            record("window active", False, reason="not activated: is the screen locked?")
            json.dump(results, open(out_path, "w"), ensure_ascii=False, indent=1, default=str)
            dialog.hide()
            return 2
        picker = dialog.taxon_picker
        field = picker.search_input
        click(field)
        record("field has focus after click", field.hasFocus())

        # 1. typing searches; the default chip is filled
        text("Flammulina vel")
        pump(500)
        entry = picker.current_entry()
        record("typing opens the results", picker.is_popup_visible(),
               first=entry.row.name if entry and entry.row else None,
               default=entry.row.concepts[entry.choice].source_name if entry and entry.row else None)
        popup = picker.popup
        field_bottom = field.mapToGlobal(QPoint(0, field.height())).y()
        record("the list floats as its own window below the field",
               popup.isWindow() and popup.frameGeometry().top() >= field_bottom,
               popup=popup.frameGeometry().getRect(), dialog=dialog.frameGeometry().getRect())
        record("the field keeps focus and the dialog stays active",
               field.hasFocus() and dialog.isActiveWindow() and QApplication.activeWindow() is dialog)

        # 2. Option+Right switches the highlighted row to COL; Enter picks it, no save
        key("right", FLAG_ALT)
        entry = picker.current_entry()
        record("Option+Right switches source", entry.row.concepts[entry.choice].source_name == "COL")
        key("return")
        pump(300)
        data = dialog.get_data()
        record("Enter picks COL and does not save", data["sporely_taxon_id"] == 98288 and not accepted
               and dialog.isVisible(), id=data["sporely_taxon_id"])
        record("focus stays in the dialog after the pick", dialog.isActiveWindow())

        # 3. the card's NorTaxa button switches
        nortaxa = next(b for b in picker.card.source_buttons if b.text().startswith("NorTaxa"))
        click(nortaxa)
        record("card Kilde click switches to NorTaxa", dialog.get_data()["sporely_taxon_id"] == 625621)

        # 4. Endre, Escape twice restores the card unchanged
        click(picker.card.change_button)
        record("Endre focuses the search", field.hasFocus(), text=field.text())
        text("Russ")
        pump(400)
        key("escape")
        record("Escape closes the results", not picker.is_popup_visible())
        key("escape")
        record("second Escape restores the card", picker.card.isVisible()
               and dialog.get_data()["sporely_taxon_id"] == 625621 and dialog.isVisible())

        # 5. Tab moves focus without picking the automatic highlight
        click(picker.card.change_button)
        key("a", FLAG_CMD)
        text("Russula")
        pump(500)
        key("tab")
        record("Tab leaves without picking", dialog.get_data()["sporely_taxon_id"] == 625621
               and not field.hasFocus() and not picker.is_popup_visible(),
               unfinished=picker.unfinished_text(), focus=type(QApplication.focusWidget()).__name__)

        # 6. Enter with the results closed never saves
        click(field)
        closed = not picker.is_popup_visible()
        key("return")
        pump(300)
        record("Enter on closed results searches and does not save", closed and not accepted
               and dialog.isVisible() and picker.is_popup_visible(), text=field.text())

        # 7. scrolling, PageDown and Show more
        key("escape")
        key("a", FLAG_CMD)
        text("Russula")
        pump(500)
        view = picker.popup.view
        popup = picker.popup
        record("a long list reaches past the dialog's bottom edge",
               popup.frameGeometry().top() >= field_bottom
               and popup.frameGeometry().bottom() > dialog.frameGeometry().bottom()
               and field.hasFocus() and dialog.isActiveWindow(),
               popup=popup.frameGeometry().getRect(), dialog=dialog.frameGeometry().getRect(),
               rows_visible=round(view.height() / 46, 1))
        if len(sys.argv) > 2:
            save_composite(dialog, popup, sys.argv[2])
        before = view.verticalScrollBar().value()
        scroll(view.viewport(), -5)
        after = view.verticalScrollBar().value()
        record("mouse wheel scrolls the results", after > before and picker.is_popup_visible()
               and field.hasFocus(), before=before, after=after)
        before = view.verticalScrollBar().value()
        glide(view.viewport(), -90)
        after = view.verticalScrollBar().value()
        record("trackpad pixel scrolling glides", 0 < after - before <= 150 and picker.is_popup_visible()
               and field.hasFocus() and dialog.isActiveWindow(), before=before, after=after)
        text("x")
        pump(400)
        record("typing continues after scrolling the list", field.text() == "Russulax" and field.hasFocus())
        key("backspace")
        pump(400)
        rows_before = len(picker.results())
        for _ in range(12):
            key("pagedown")
        key("up")  # the last row is "use as a name without a taxon"; Show more is above it
        entry = picker.current_entry()
        record("PageDown reaches Show more", entry is not None and entry.kind == "more", rows=rows_before)
        key("return")
        pump(300)
        record("Show more appends", len(picker.results()) > rows_before and picker.is_popup_visible()
               and field.hasFocus(), rows=len(picker.results()))

        # 8. clicking a COL chip in the results picks COL
        key("a", FLAG_CMD)
        text("Flammulina velutipes")
        pump(500)
        model = picker.model
        index = next(model.index(i, 0) for i in range(model.rowCount())
                     if model.index(i, 0).data(picker_role()).kind == "row"
                     and model.index(i, 0).data(picker_role()).row.name == "Flammulina velutipes")
        rect = view.visualRect(index)
        from PySide6.QtWidgets import QStyleOptionViewItem
        option = QStyleOptionViewItem()
        option.rect = rect
        option.font = view.font()
        _pill, chips = picker._delegate.chip_rects(option, index.data(picker_role()))
        col = next(c for c, concept in zip(chips, index.data(picker_role()).row.concepts) if concept.source_name == "COL")
        picks: list[int] = []
        picker.concept_picked.connect(lambda _row, concept: picks.append(concept.sporely_taxon_id))
        click(view.viewport(), col.center().toPoint())
        record("COL chip click picks COL once", picks == [98288]
               and dialog.get_data()["sporely_taxon_id"] == 98288 and not picker.is_popup_visible(),
               picks=picks)
        record("the dialog stays active after clicking in the list", dialog.isActiveWindow())

        # 8b. a press on a non-focusable part of the dialog closes the list
        click(picker.card.change_button)
        key("a", FLAG_CMD)
        text("Russula")
        pump(500)
        label = next(w for w in dialog.findChildren(QLabel) if w.isVisible() and w.focusPolicy() == Qt.NoFocus
                     and w.window() is dialog and not picker.isAncestorOf(w) and w.width() > 20)
        click(label)
        record("a press outside the list closes it", not picker.is_popup_visible(), on=label.text()[:30])
        click(field)
        key("escape")  # the list is closed, so this restores the card
        record("Escape after an outside press restores the card", picker.card.isVisible()
               and dialog.isVisible() and dialog.get_data()["sporely_taxon_id"] == 98288)

        # The Save prompt for unfinished text is not driven here: a modal box
        # opened by synthetic input never becomes the key window, so input
        # cannot reach it. tests/test_observation_details_taxon_picker.py
        # covers its answers; the owner's checklist covers it by hand.

        # 10. an Artsorakel chip applies its resolved concept
        for _ in range(2):  # close the results, then restore the card; never Escape the dialog itself
            if not picker.card.isVisible():
                key("escape")
        resolved = [b for b in picker.ai_buttons if b.property("resolved") and b.isVisible()]
        record("an Artsorakel chip is offered for the photo", bool(resolved), chips=len(picker.ai_buttons))
        if resolved:
            click(resolved[0])
            data = dialog.get_data()
            record("resolved Artsorakel chip applies NorTaxa with provider proof",
                   data["sporely_taxon_id"] == 625621 and data["taxon_identity_proof"] == "external_id_resolution",
                   proof=data["taxon_identity_proof"])

        # 11. closing the dialog closes an open list
        click(picker.card.change_button)
        text("Russ")
        pump(500)
        was_open = picker.is_popup_visible()
        dialog.hide()
        pump(200)
        record("hiding the dialog closes the list", was_open and not picker.popup.isVisible())
        dialog._cleanup_dialog_threads()
    json.dump(results, open(out_path, "w"), ensure_ascii=False, indent=1, default=str)
    return 0 if all(r["ok"] for r in results) else 1


def picker_role():
    from ui.taxon_picker import ROLE_ENTRY
    return ROLE_ENTRY


if __name__ == "__main__":
    if sys.platform != "darwin":
        raise SystemExit("macOS only")
    sys.exit(main(sys.argv[1]))
