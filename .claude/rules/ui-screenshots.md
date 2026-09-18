---
paths:
  - "tools/review_ui/**"
  - "tools/render_review_screenshots*"
  - "ui/**/*.py"
  - "widgets/**/*.py"
---

# UI screenshot review evidence

- Do not create a new screenshot renderer for each feature. Add scenarios to the generic UI review renderer under `tools/review_ui/scenarios/`; scenario code should construct meaningful states with real production widgets and deterministic, no-network fixtures.
- Run from the current worktree root using the canonical environment below.
  List the registered scenario IDs and groups with:

  ```bash
  /Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python -m tools.render_review_screenshots --list
  ```

- Render a focused group or one or more scenarios with:

  ```bash
  QT_QPA_PLATFORM=offscreen /Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python -m tools.render_review_screenshots --group reference-library <output-dir>
  QT_QPA_PLATFORM=offscreen /Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python -m tools.render_review_screenshots --scenario reference.add-range --scenario reference.dark <output-dir>
  ```

- UI-affecting features should normally add or update their relevant review scenarios. Keep feature-specific fixture construction beside those scenarios; the shared renderer owns Qt setup, themes/locales, capture, cleanup, output confinement, and manifest generation.
- When asked for UI screenshot evidence, run the repository-owned deterministic renderer with an explicit disposable output directory:

  ```bash
  QT_QPA_PLATFORM=offscreen /Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python -m tools.render_review_screenshots --scenario <scenario-id> <output-dir>
  ```

- The renderer uses mocked fixtures and offscreen Qt widgets. It must not use production credentials, live cloud/database data, or arbitrary desktop capture.
- Treat `<output-dir>/manifest.json` as authoritative. Inspect only the PNG/JPEG files it lists, using the current agent environment's image-viewing capability.
- The renderer is also configured for autonomous review through the repository's `.autonomous-development.toml`; the framework supplies its run-owned output directory automatically.

## Scope limit: screenshots are evidence about layout, not behavior

Screenshots are static evidence. They show layout — clipping, alignment, elision, contrast, character rendering — and nothing about behavior. Signal loops, checkbox flicker, focus changes, scroll retention across a rebuild, drag, resize, and state surviving a restart are not visible in a screenshot and need a unit test or the user's own hands. Do not offer a clean screenshot as evidence that an interaction works.

Automated behavior tests supplement screenshot evidence. They do not waive the
repository's human-gated verification requirements in `AGENTS.md`.
