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
## Cloud sync invariants

- Treat `sync_images`, `materialize_remote_images`, and `full_pull` as independent controls. Never turn all three on as a generic "full sync" fix.
- Observations-tab refresh uses `sync_images=False`, `materialize_remote_images=True`, and `full_pull=False`: push metadata, fast-pull only new/changed remote observations, and download their missing media without scanning the local media backlog.
- Profile & Cloud "Sync now" uses `sync_images=True`, `materialize_remote_images=True`, and `full_pull=False`: upload genuinely pending selected local media and materialize new/changed remote media, while preserving unchanged-observation pruning.
- `full_pull=True` is a deep reconciliation/recovery control. Pair it with `sync_images=False` unless a separately named, user-confirmed migration explicitly requires scanning and uploading all pending local media.
- `sync_images=True` activates the global pending-image dirty scan. It may re-dirty synced observations containing eligible `cloud_id IS NULL` rows and must never be enabled for background/startup/ordinary refresh paths.
- `materialize_remote_images=True` controls remote byte download; it does not require `sync_images=True` or `full_pull=True`.
- Rows with `source_role=cloud_recovery_cache` or `file_purpose=cache` are remote-owned recovery copies. They may receive metadata/link repairs but their bytes must never be prepared or uploaded back to cloud.
- Preserve the no-op fast-path contract: unchanged observations must not trigger bulk image/measurement fetches, WebP preparation, measurement pushes, or mosaic rebuilds.
- Any sync flag or media-selection wiring change must include focused tests for the exact caller mode and run `tests/test_cloud_sync_fast_path.py`, `tests/test_cloud_sync_dirty_loop_steady_state.py`, and the affected media pull/upload policy tests.
