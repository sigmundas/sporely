## Working agreements

- Do not run `npm test` after modifying JavaScript files.
- Ask for confirmation before adding new production dependencies.
- Do not run heavy build steps, including Capacitor syncs, PyInstaller, Docker builds, full app builds, packaging commands, or dependency installation, unless explicitly requested.
- Keep patches narrow. If a task touches multiple workflows or large UI files, propose staged patches and stop after the current stage.
- Do not rewrite or refactor unrelated code while fixing a bug. Preserve existing behavior unless the prompt explicitly asks for a behavior change.
- For sporely-py, always use the project virtual environment:
/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python
/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/pytest
Ask for confirmation before installing or upgrading packages in .venv.
For syntax checks, use ./.venv/bin/python -m py_compile <touched files>

## Localization

Development is in English. Translations must be current before publish.
Norwegian Bokmål (`nb_NO`), Swedish (`sv_SE`) and German (`de_DE`) are
supported; German uses informal "du".

- Any translatable UI string must be wrapped in Qt's `self.tr("…")`
  (or `QCoreApplication.translate(…)` in non-widget code). Never
  hardcode a bare English literal in a widget's user-facing surface.
- After adding or changing any `tr("…")` call, run:
  ```bash
  ./tools/update_translations.sh
  ```
  This calls `.venv/bin/pyside6-lupdate` to refresh
  `i18n/Sporely_nb_NO.ts`, `Sporely_sv_SE.ts`, `Sporely_de_DE.ts`, then
  `.venv/bin/pyside6-lrelease` to compile the matching `.qm` binaries
  that the app loads at runtime. Both `.ts` and `.qm` files must be
  committed together.
- New or changed strings appear in the `.ts` files as
  `<translation type="unfinished">`. To hand these to a translation
  agent, run:
  ```bash
  ./.venv/bin/python tools/agent_translate.py
  ```
  which extracts every unfinished/empty message into
  `missing_translations.json`. Fill in the JSON (or hand it to an
  agent), then merge the results back into the `.ts` files and re-run
  `update_translations.sh` so the `.qm` binaries pick up the new
  translations.
- Do not leave `<translation type="unfinished">` entries in a published
  build. If a language cannot be translated in the current task, note
  it explicitly in the task report so a follow-up agent can finish it.
- Do not translate: product names (`Sporely`, `Sporely Pro`), domains
  (`sporely.no`, `app.sporely.no`), third-party product names
  (`iNaturalist`, `Artsobservasjoner`, `Artdatabanken`, `Artsorakel`,
  `Google Play`, `Stripe`, `Obtanium`), file-format acronyms, or
  scientific names.
- The `tools/update_translations.sh` file list determines which source
  files `lupdate` scans. If you add a new Python module that contains
  user-facing `tr(…)` calls, add it to that list.