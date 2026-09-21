---
paths:
  - "i18n/**"
  - "tools/update_translations.sh"
  - "tools/agent_translate.py"
  - "ui/**/*.py"
  - "widgets/**/*.py"
---

# Localization

Development is in English. Translations must be current before publish. Norwegian Bokmål (`nb_NO`), Swedish (`sv_SE`) and German (`de_DE`) are supported; German uses informal "du".

- Any translatable UI string must be wrapped in Qt's `self.tr("…")` (or `QCoreApplication.translate(…)` in non-widget code). Never hardcode a bare English literal in a widget's user-facing surface.
- After adding or changing any `tr("…")` call, run:

  ```bash
  PATH="/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin:$PATH" bash tools/update_translations.sh
  ```

  Run from the current worktree root. The script uses local `.venv/bin` tools
  when present, otherwise the canonical environment on `PATH`; it calls `pyside6-lupdate` to refresh `i18n/Sporely_nb_NO.ts`, `Sporely_sv_SE.ts`, `Sporely_de_DE.ts`, then `pyside6-lrelease` to compile the matching `.qm` binaries that the app loads at runtime. Both `.ts` and `.qm` files must be committed together.
- New or changed strings appear in the `.ts` files as `<translation type="unfinished">`. To hand these to a translation agent, run:

  ```bash
  /Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python tools/agent_translate.py
  ```

  which extracts every unfinished/empty message into `missing_translations.json`. Fill in the JSON (or hand it to an agent), then merge the results back into the `.ts` files and re-run the translation command above so the `.qm` binaries pick up the new translations.
- Do not leave `<translation type="unfinished">` entries in a published build. If a language cannot be translated in the current task, note it explicitly in the task report so a follow-up agent can finish it.
- Do not translate: product names (`Sporely`, `Sporely Pro`), domains (`sporely.no`, `app.sporely.no`), third-party product names (`iNaturalist`, `Artsobservasjoner`, `Artdatabanken`, `Artsorakel`, `Google Play`, `Stripe`, `Obtanium`), file-format acronyms, or scientific names.
- The `tools/update_translations.sh` file list determines which source files `lupdate` scans. If you add a new Python module that contains user-facing `tr(…)` calls, add it to that list.
- Avoid reading generated `.qm` files or bulk-generated `.ts` churn. Inspect only the source messages and relevant translation entries.

Translation extraction/compilation is a focused generated-artifact update, not
a full application build. Keep unrelated translation churn out of the patch.
