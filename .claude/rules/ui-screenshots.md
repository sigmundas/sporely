---
paths:
  - "tools/review_ui/**"
  - "tools/render_review_screenshots*"
  - "ui/**/*.py"
  - "widgets/**/*.py"
---

# UI verification evidence

Agents verify UI work themselves; the user gives a final opinion on layout (see
the verification tiers in `AGENTS.md`). Layout evidence comes from the renderer,
behavior evidence from tests and real-input runs.

## Layout: the review renderer

- Do not create a new screenshot renderer for each feature. Add scenarios to the generic UI review renderer under `tools/review_ui/scenarios/`; scenario code should construct meaningful states with real production widgets and deterministic, no-network fixtures.
- UI-affecting features add or update their review scenarios. Keep feature-specific fixture construction beside those scenarios; the shared renderer owns Qt setup, themes/locales, capture, cleanup, output confinement, and manifest generation.
- Run from the current worktree root with an explicit disposable output directory:

  ```bash
  /Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python -m tools.render_review_screenshots --list
  QT_QPA_PLATFORM=offscreen /Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python -m tools.render_review_screenshots --group reference-library <output-dir>
  QT_QPA_PLATFORM=offscreen /Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python -m tools.render_review_screenshots --scenario reference.add-range --scenario reference.dark <output-dir>
  ```

- The renderer uses mocked fixtures and offscreen Qt widgets. It must not use production credentials, live cloud/database data, or arbitrary desktop capture.
- Treat `<output-dir>/manifest.json` as authoritative and open every PNG/JPEG it lists for the touched scenarios.
- Inspect, don't just render. Check clipping, elision, alignment, spacing,
  contrast and character rendering, and compare against the conventions in
  `docs/development/gui-conventions.md`. For layout changes, cover the states
  that break layouts: a small and a large window size, the dark theme, and a
  long-string locale (`de_DE` or `nb_NO`) via the context's `set_theme` and
  `locale` helpers. Fix what you find before reporting.
- Report what the screenshots show, and attach the relevant ones (before/after
  for visible changes) so the user can give their layout opinion.
- The renderer is also configured for autonomous review through the repository's `.autonomous-development.toml`; the framework supplies its run-owned output directory automatically.

## Behavior: tests and real input

Screenshots are static. They show nothing about signal loops, checkbox flicker,
focus changes, scroll retention across a rebuild, drag, resize, or state
surviving a restart. Prove those with:

- **pytest-qt tests on real production widgets.** Drive them with `qtbot`
  (clicks, key presses, `waitSignal`/`waitUntil`) and assert on resulting widget
  and database state. Test restart survival by persisting, destroying and
  recreating the widget from settings or the database. Mock only what the claim
  does not depend on.
- **A cocoa real-input run for macOS focus, popup and keyboard behavior.**
  Offscreen Qt does not reproduce macOS popup focus order. Run with
  `QT_QPA_PLATFORM=cocoa` and post events only to the test process
  (`CGEventPostToPid` for keys; NSEvent via `[NSApp postEvent:atStart:]` for
  mouse). Never use System Events `keystroke`/`key code`: the user may be
  working, and those keys land in whatever app is frontmost. Check the screen is
  unlocked first, and count a failure as environmental only when activation was
  lost. `tools/review_ui/taxon_picker_mac_check.py` is the reference harness.

A clean screenshot is never evidence that an interaction works, and a
real-input run is an agent check, not the user's hands-on verification.
