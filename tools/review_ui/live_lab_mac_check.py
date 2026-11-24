"""Drive Live Lab's capture-state bar, chips and section headings with real macOS input.

Offscreen Qt does not reproduce macOS focus, activation and shortcut routing,
so this check runs the real ``LiveLabTab`` on the cocoa platform, isolated by
the review renderer's Live Lab fixtures (temporary database and profile, a
drop-folder watcher, no cloud). Input stays in THIS process, with the helpers
of ``tools.review_ui.taxon_picker_mac_check``: keys through CoreGraphics
``CGEventPostToPid``, clicks as NSEvents queued in this application's own
event queue. A run whose window never became active (a locked screen, say)
is reported as environmental and exits 2.

The window comes to the front for about twenty seconds. From the worktree:

    QT_QPA_PLATFORM=cocoa .venv/bin/python -m tools.review_ui.live_lab_mac_check out.json

These are agent checks with synthetic events, not the owner's hands-on
verification.
"""
from __future__ import annotations

import json
import sys

from PySide6.QtWidgets import QApplication

from tools.review_ui import taxon_picker_mac_check as mac

import ctypes
import time

# macOS virtual key codes and the characters they type (ANSI layout).
_KEYS = {
    "1": (18, "1"), "2": (19, "2"), "3": (20, "3"), "b": (11, "b"), "d": (2, "d"), "i": (34, "i"),
    "p": (35, "p"), "space": (49, " "), "return": (36, "\r"), "right": (124, "\uf703"),
    "left": (123, "\uf702"),
}
_TYPED = {" ": "space"}
for _ch in "abcdefghijklmnopqrstuvwxyz0123456789":
    _TYPED.setdefault(_ch, _ch)


def _nsstring(value: str):
    return mac._msg(ctypes.c_void_p, mac._objc.objc_getClass(b"NSString"), b"stringWithUTF8String:",
                    value.encode("utf-8"), argtypes=(ctypes.c_char_p,))


def _post_key(widget, code: int, chars: str) -> None:
    """A native key NSEvent queued in THIS application's own event queue.

    ``CGEventPostToPid`` keys need the posting process to be trusted for
    Accessibility; an NSEvent posted to NSApp does not, and Cocoa still sends
    it to the key window through its normal dispatch, like a real key press.
    """
    _window, number = mac._ns_window(widget)
    for kind in (10, 11):  # NSEventTypeKeyDown, NSEventTypeKeyUp
        event = mac._msg(
            ctypes.c_void_p, mac._objc.objc_getClass(b"NSEvent"),
            b"keyEventWithType:location:modifierFlags:timestamp:windowNumber:context:characters:"
            b"charactersIgnoringModifiers:isARepeat:keyCode:",
            kind, mac.CGPoint(0, 0), 0, time.monotonic(), number, None, _nsstring(chars), _nsstring(chars),
            False, code,
            argtypes=(ctypes.c_ulong, mac.CGPoint, ctypes.c_ulong, ctypes.c_double, ctypes.c_long,
                      ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_bool, ctypes.c_ushort),
        )
        mac._post_ns(event)
        mac.pump(40)
    mac.pump(80)


_TARGET: list = []


def key(name: str) -> None:
    code, chars = _KEYS[name]
    _post_key(_TARGET[0], code, chars)


def text(value: str) -> None:
    for ch in value:
        name = _TYPED[ch]
        code, _chars = _KEYS.get(name, (0, ch))
        _post_key(_TARGET[0], code, ch)


def main(out_path: str) -> int:
    from tools.review_ui.context import ReviewContext
    from tools.review_ui.scenarios import live_lab as sc

    from ui.styles import apply_palette, get_style

    app = QApplication.instance()
    with ReviewContext(app) as ctx:
        # Style the app as main.py does, so layouts behave as for the user.
        app.setStyle("Fusion")
        apply_palette("light")
        app.setStyleSheet(get_style("light"))
        tab, _profile = sc._live_lab_tab(ctx, session_mode="offline")
        area = app.primaryScreen().availableGeometry()
        tab.resize(min(1440, area.width() - 80), min(900, area.height() - 80))
        tab.move(area.x() + 40, area.y() + 20)
        tab.show()
        mac._windows.append(tab)
        _TARGET.append(tab)
        mac.pump(400)
        if not mac.front(tab):
            mac.record("window active", False, reason="not activated: is the screen locked?")
            json.dump(mac.results, open(out_path, "w"), ensure_ascii=False, indent=1, default=str)
            return 2
        mac.record("window active", True)
        sc._start(tab)
        review_calls: list[str] = []
        tab._handle_raw_review_shortcut = lambda action: review_calls.append(action)

        # 1. number and letter shortcuts with the viewer focused
        mac.click(tab.live_image_label)
        for name, expected in (("1", "40X"), ("2", "63X"), ("3", "100X")):
            key(name)
            mac.record(f"key {name} chooses objective {expected}", tab.objective_combo.selected_value() == expected,
                       got=tab.objective_combo.selected_value())
        for name, expected in (("b", "BF"), ("p", "Phase"), ("i", "DIC")):
            key(name)
            mac.record(f"key {name.upper()} chooses contrast {expected}", tab.contrast_combo.selected_value() == expected,
                       got=tab.contrast_combo.selected_value())
        mac.record("the live label follows", tab.live_state_label.text().startswith("100x · DIC"),
                   label=tab.live_state_label.text())

        # 2. the same letters type into a text field
        mac.click(tab.session_note_input)
        text("bip 2")
        mac.record("letters and digits type into the note field", tab.session_note_input.text() == "bip 2"
                   and tab.contrast_combo.selected_value() == "DIC" and tab.objective_combo.selected_value() == "100X",
                   note=tab.session_note_input.text(), contrast=tab.contrast_combo.selected_value())
        tab.session_note_input.clear()

        # 3. bar buttons by mouse
        mac.click(tab.capture_state_bar.objective_segments.button_for_value("40X"))
        mac.click(tab.capture_state_bar.contrast_segments.button_for_value("Phase"))
        mac.record("bar buttons choose objective and contrast",
                   (tab.objective_combo.selected_value(), tab.contrast_combo.selected_value()) == ("40X", "Phase"))

        # 4. section heading: click, then Space and Return on the focused heading
        section = tab.slide_prep_group
        before = section.is_expanded()
        mac.click(section.header)
        mac.pump(300)
        mac.record("clicking the Preparation heading toggles it", section.is_expanded() is not before
                   and section.header.hasFocus(), focus=type(QApplication.focusWidget()).__name__)
        for name in ("space", "return"):
            state = section.is_expanded()
            key(name)
            mac.pump(300)
            mac.record(f"{name} on the focused heading toggles it", section.is_expanded() is not state)
        if not section.is_expanded():
            key("space")
            mac.pump(300)

        # 5. chips. On macOS a click does not give a button keyboard focus
        # (platform convention), so the row is reached with Tab under Full
        # Keyboard Access; the check gives the chip focus directly and then
        # drives it with real keys.
        mount = tab.mount_combo
        water = mount.button_for_value("Water")
        mac.record("the Preparation section is open for the chip steps", section.is_expanded() and water.isVisible(),
                   expanded=section.is_expanded(), visible=water.isVisible(), body=section.body.isVisible(),
                   selected=mount.selected_value(), geometry=str(water.geometry()),
                   row_width=mount.width(), contents=mount.contentsRect().width(),
                   chips=mount.visible_chip_values(), window=str(tab.size()),
                   panel=tab.slide_prep_group.width())
        mac.click(water)
        mac.record("clicking a chip chooses it", mount.selected_value() == "Water", selected=mount.selected_value())
        chip = mount.button_for_value("Water")
        chip.setFocus()
        mac.pump(100)
        key("right")
        moved = QApplication.focusWidget()
        mac.record("Right moves focus to the next chip without choosing", moved is not chip
                   and moved in mount.buttons() and mount.selected_value() == "Water",
                   focus=getattr(moved, "text", lambda: "")())
        key("space")
        chosen = mount.selected_value()
        mac.record("Space chooses the focused chip", chosen not in ("Water", "Not_set"), chose=chosen)
        key("space")
        mac.record("Space on the chosen chip clears it", mount.selected_value() == "Not_set")
        key("2")
        mac.record("a number key still chooses an objective while a chip has focus",
                   tab.objective_combo.selected_value() == "63X")

        mac.record("no key reached the RAW review shortcuts", review_calls == [], calls=review_calls)
        tab.stop_session()
        tab.close()
        mac.pump(300)
    json.dump(mac.results, open(out_path, "w"), ensure_ascii=False, indent=1, default=str)
    if any(r.get("environmental") for r in mac.results if not r["ok"]):
        return 2
    return 0 if all(r["ok"] for r in mac.results) else 1


if __name__ == "__main__":
    if sys.platform != "darwin":
        raise SystemExit("macOS only")
    sys.exit(main(sys.argv[1]))
