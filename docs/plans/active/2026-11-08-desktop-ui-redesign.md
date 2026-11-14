# Desktop UI redesign

Status: **direction approved; stage 0 in review (sigmundas/sporely#65).** Rewritten 2026-11-08
against `main` @ `a6d2376` to follow the owner's plan doc, then updated with
the owner's answers the same day.

## Sources

- **Plan doc (authoritative for design):** “Sporely UI redesign: plan”,
  <https://claude.ai/artifact/AYaXqCtHpxonbCxzBjJtBA>. Its sections *Target
  structure*, *Workflow*, *Stages*, *Open decisions* and *Decisions from the
  mockup review (2026-11-08)* are summarized below; where this file and the
  doc disagree, the doc wins and the difference goes under *Open questions*.
- **Mockups:**
  `/Users/sigmundas/Documents/Code/sporely/sporely-py/docs/plans/november-ui/mockups/`
  (`Bilder-Live`, `Bilder-Logg`, `Bilder-Filer`, `Maal`, `Analyse-Referanse`,
  `Observasjon-Rediger`). Design-tool markup: read them for layout and
  behaviour, not as code. The canvas is at
  <https://claude.ai/artifact/QVq4fu3hQhMzcwCynR4nk1>.
- **Screenshots of today's app:**
  `/Users/sigmundas/Documents/Code/sporely/sporely-py/docs/plans/november-ui/`.
- That folder exists only in the main checkout and is gitignored
  (`.gitignore:188`). The repo is public, the screenshots contain personal
  profile data and paths, and the mockups contain sample observation data with
  coordinates. Reference them by absolute path; commit nothing from the folder
  and publish none of it (decided 2026-11-08).

## Goal

Make every screen show what the current task needs, and make microscope
settings impossible to miss. Deliver it in small stages, with the app fully
usable after each one.

- **Less density:** each control gets a visibility level (below).
- **Fewer large modal dialogs:** in-place modes, collapsed sections with a
  summary line, slide-in drawers.
- **Fewer capture errors:** the current objective and contrast method are
  always prominent; a wrong objective is visible before capturing; settings
  are stored per image and can be reviewed and corrected later.
- **Consistency:** drawers, collapsed sections and staging look and behave the
  same on every screen.
- **No regressions:** no data model change outside stage 8; existing keyboard
  shortcuts keep working.

Out of scope: visual theme changes (Clinical Nocturne and Slate Lab in
`ui/styles.py` stay), the web app and community site, new analysis features,
and data model, sync and cloud-contract changes outside what stage 8 is
approved to change.

## Design rules

| Level | Use when | Examples |
| --- | --- | --- |
| Docked | Used every few seconds or minutes | Measurement tools, Målinger list, objective and contrast bar, RAW essentials |
| Collapsed with summary line | Set once per session or slide, but the value must stay visible | Preparat (“L4 · Fersk · Sporavtrykk”), Skala, Notes, RAW “Mer” |
| Drawer | An occasional task | AI-forslag, reference picker, Visning and Info in Mål, plot settings |
| Dialog | Rare setup or data entry | Kalibrer objektiv, new reference, Sync Shot, Preferanser |

- **Summary lines are complete:** a value someone opens a section only to read
  belongs in its summary.
- **Drawers:** they slide in next to what opened them (owner, 2026-11-09:
  the Analyse reference drawer opens at the plot's left edge, beside “Legg
  til referanse…”; Mål's Visning and Info open at the right, beside their
  buttons). They overlay the content without resizing the layout, one at a
  time, and close with Esc, the close button, or by finishing the task.
- **Hide what doesn't apply:** microscope fields are hidden for field images;
  warnings appear only when true.
- **Staging vs. saved:** images not yet part of an observation (Live,
  Kameraimport) have an orange border; saving moves them into the
  observation's image strip.
- **Colour means objective,** from the standard objective ring colours (4x
  red, 10x yellow, 20x green, 40x light blue, 63x dark blue, 100x white),
  defined once and used in the capture bar, the live-view label and every
  thumbnail badge. Unselected objectives show their colour as a stripe.
  Contrast method is shown as text. The ring standard (ISO 8578) sets each
  objective's default colour by magnification; an objective's own stored
  colour still overrides it; macro profiles are neutral grey (P3).
- **Motion:** 150–250 ms ease-out; animation never blocks input.
- **The image frame shows only what you need while looking through the
  microscope** (owner, 2026-11-09): the live-view label, the scale bar, the
  capture-state bar and the session control. Actions on captures stay off
  the image: copying RAW settings goes to the RAW panel (stage 5), saving to
  the staging strip (stage 9).

## Target structure

The end state separates work on a saved observation from the staging areas
where images arrive.

- **Observasjoner is the whole app for one observation;** no other top-level
  tabs.
  - Modes in workflow order: Vis, Rediger, Bilder, Mål, Analyse
    (shortcuts ⌥O, ⌥L, ⌥M, ⌥A; ⌥I and ⌘E/⌥E stay as aliases, see
    *Decisions*).
  - Observation list on the left: docked in Vis and Rediger, a thin rail in
    Bilder, Mål and Analyse; clicking the rail opens it as a left drawer.
  - One shared image strip below the modes, the same in every mode; unsaved
    images first, with an orange border.
  - Tabell: a full-width sortable overview opened from the list header;
    clicking a row opens that observation.
- **Bilder** (staging) adds images from two sources that share the staging
  strip, the microscope bar, Preparat and “Lagre til observasjon”:
  - **Live (mikroskop)**, today's Live Lab: objective and contrast as a
    floating bar over the live view, label and scale bar on the image,
    Preparat and Notater as summary chips. The RAW panel exists only when the
    camera delivers RAW, is docked beside the image by default, and its pin
    switches docked/overlay (remembered).
  - Live has two session types, chosen at session start (default in
    Preferanser › Bildeimport): **Kamera tilkoblet** (watches a folder, shows
    the live view) and **Kun logg** (no live view; the settings bar is the main
    element, every change and note is logged with its time, Sync Shot's QR code
    is shown at the start, and the session control — running time plus a red
    “Avslutt økt” — is prominent). Kun logg photos arrive later through
    Kameraimport. The mode bar shows “Live” or “Logger”. Today these are the
    `live` and `offline` session modes in `ui/live_lab_tab.py`
    (`SESSION_MODE_LIVE`, `SESSION_MODE_OFFLINE`).
  - **Filer og SD-kort**, today's Legg til bilder: Felt or Mikroskop per
    selected image (toggle on the image, top left). The microscope bar appears
    only for microscope images and applies to all selected. Field images show
    time and GPS as one line with “Bruk for observasjonen”. Crop and AI crop
    are image tools.
  - A running Live session survives switching mode.
- **Analyse** is a mode. Comparing with other taxa goes through the reference
  picker. A strip of spore thumbnails with length × width sits under the plot.
- **Kameraimport** matches a folder of images to many observations, so it stays
  its own screen: a distinct button at the far right of the mode bar (⌥C) with
  a camera icon, “alle observasjoner” and a count of photos waiting.
  Matches land as unsaved images in each observation's Bilder strip, marked
  “fra kamera”, and are reviewed and saved there. Session logs are kept after
  import (the only record of each photo's settings). Sync Shot stays a dialog
  and is also shown when a Kun logg session starts.
- **Rediger** (from the mockup review): AI suggestions inline next to
  Taksonomi, each with “Bruk”; the map collapsed to one line; latitude and
  longitude on one row; both comment fields always visible; a “Status og
  publisering” section: Utkast → Ferdig → Publisert til tjeneste, with one
  main button “Ferdig og publiser til …”. Ferdig only clears the draft flag
  (`is_draft`); visibility (Privat, Venner, Offentlig — `sharing_scope`)
  stays its own explicit setting next to the button and is never changed
  implicitly. It replaces the Utkast checkbox and the Publiser menu; the
  header shows a status badge.
- **Preparat** choices are chip rows, one per group (Medium, Farging with a dot
  in the reagent's colour, Tilstand, Kilde): one click selects, a second
  clears, nothing selected means not set (no “–” option). Most-used first;
  “+ N flere” opens the rest in place, never as a dropdown. The lists come from
  Mikroskoptagger in Preferanser (`ui/database_settings_dialog.py`).

What happens to today's dialogs (the doc covers these; stage 10 sorts the
rest):

| Today | Becomes | Stage |
| --- | --- | --- |
| Rediger observasjon (`ObservationDetailsDialog`) | Rediger mode; AI-forslag inline next to Taksonomi; both comment fields always visible | 9 |
| Legg til bilder (`ImageImportDialog`) | Bilder mode, source Filer og SD-kort; EXIF to an Info drawer shown only when EXIF exists; Tid og GPS one line for field images; Nyquist resizing to Preferanser › Bildeimport, off by default | 9 |
| Legg til referanse (`AddReferenceDialog`) | Picking: a drawer over the Analyse plot, beside “Legg til referanse…”. Creating: two-step dialog | 7 |
| Kalibrer objektiv, Preferanser, Sync Shot | Stay dialogs | — |
| Live Lab, Mål and Analyse tabs | Modes in Observasjoner; Live Lab becomes the Live source of Bilder | 9 |
| Referansebibliotek | Stays a dialog; UUID, citation key and range IDs move behind “Tekniske detaljer” | 10 |

## Current state (verified 2026-11-08 @ `a6d2376`)

- **Dialogs:** 34 dialog classes in `ui/` (29 direct `QDialog` subclasses,
  5 via `GeometryMixin`: `ObservationDetailsDialog`, `ImageImportDialog`,
  `CalibrationDialog`, `AddReferenceDialog`, `ReferenceAddDialog`), 17 inline
  `QDialog(...)` builds, 176 `QMessageBox` calls (112 in `ui/main_window.py`,
  30 in `ui/calibration_dialog.py`), 35 `QFileDialog`/`QInputDialog` uses.
- **Live Lab:** `ui/live_lab_tab.py` is 8,965 lines. The RAW edit/preview
  machinery (`PendingRawCapture`, `RawEditSession`, the preview and commit
  workers, `RawCurvePreviewWidget`, lines ~156–700) and the Microscope,
  Slide / prep and Notes cards (`_build_ui`, from line ~952) live in it. The
  RAW sliders are already shared: `ui/raw_processing_controls.py`
  (`RawProcessingControls`, also used by `ui/image_import_dialog.py`).
- **Reuse targets:** `ui/section_card.py` (`create_section_card`),
  `ui/segmented_selector.py`, `ui/adaptive_choice_selector.py`. No
  collapsible-section or drawer widget exists yet.
- **Objective colours exist twice:** `MainWindow._objective_color_for_name`
  (`ui/main_window.py:15359`) and `objective_color`
  (`ui/adaptive_choice_selector.py:733`). Both map 4–5x red, 10x yellow,
  16–32x green, 40–50x blue, 63x dark blue, 100x near-white, plus 6x orange,
  and the second one lets an objective record carry its own colour.
- **Session log:** Live already writes every dropdown change and manual note
  to `session_logs` via `_log_session_event` / `_log_dropdown_change`
  (`ui/live_lab_tab.py:7615`). The table is local SQLite plus the portable
  archive; it is not cloud-synced.
- **Tab shortcuts** (`ui/main_window.py:6079`): ⌥O/⌥M/⌥A/⌥L switch tabs;
  Camera Import is ⌥C from Measure, Analysis, Live Lab and Ingestion Hub but
  ⌥I from Observations. ⌘E / ⌥E opens Rediger observasjon.
- **Measurement rescaling exists four times:**
  `ImageImportDialog._rescale_measurements_for_image`,
  `CalibrationDialog._prompt_recalculate_measurements`,
  `ObservationsTab._rescale_measurement_lengths`, and
  `recalculate_measurements_for_objective` /
  `recalculate_measurements_for_calibration` in `database/models.py`.
- **Tests and screenshots:** 1 test file uses `qtbot`; pytest-qt 4.5.0 is
  installed in `.venv` and pinned in CI but in no requirements file.
  Live Lab tests: `test_live_lab_raw_controls.py` only. Renderer groups
  (`tools/review_ui/scenarios/`): conflicts, measure, observations,
  portable_import, raw_processing, references, taxon_picker — none for Live
  Lab or Kameraimport.
- **Style:** 576 hex colours outside `ui/styles.py`, 225 `setStyleSheet`
  calls, 103 `setFixed*` calls.

## Workflow

Live Lab is the pilot screen: the new patterns are built and tried there in
real microscopy work before they spread. The pilot stages (1–5) run in
today's Live Lab tab; the structural change (modes, Bilder with Live, Kun logg
and Filer sources, Kameraimport as its own screen) is stage 9.

- **One stage at a time,** each a commit on the `ui/redesign` branch, with its
  plan and doc updates on the same branch. No pull requests. The owner merges
  `ui/redesign` into `main` at milestones; when `main` changes, merge `main`
  into `ui/redesign`. The app builds, runs and is usable after every stage. No
  big-bang rewrite.
- **Tests:** locally, run only the stage's tests and the tests for the modules
  it touches. CI runs the full suite on every push.
- **The owner tests each stage on real work** before the next one starts;
  feedback can change later stages.
- **Commit rule (decided 2026-11-08):** a stage whose claims can be verified
  with tests and inspected renderer screenshots is committed and pushed. A
  pilot stage whose result needs interaction or a real microscope check stops
  uncommitted, with a “Manual tests” list for the owner; it is committed after
  the owner confirms.
- **Shared components are built once and reused:** collapsible section with
  summary line, slide-in drawer (left and right), the capture-state bar, the
  objective colour palette, the Preparat chip rows.

Rules for the implementing agent (the doc's rules, adapted to `AGENTS.md`):

1. Change only what the current stage lists. Note anything else under “Open
   questions” in the report instead of fixing it.
2. No data model or database changes, except in stage 8 after the owner's
   approval. New `SettingsDB` keys for remembered UI state are not schema
   changes, but name each one in the report.
3. Keep existing keyboard shortcuts working; check new ones against
   `ui/main_window.py:6079` and the per-screen `QShortcut`s.
4. Translations: every new or changed UI string goes through `tr()` with an
   English source and translations in all three shipped translations
   (`nb_NO`, `sv_SE`, `de_DE`), per `.claude/rules/localization.md`. No bare
   literals. Run `tools/update_translations.sh` once per stage; commit `.ts`
   and `.qm` together; no `unfinished` entries left. New modules with `tr()`
   calls are added to the script's file list.
5. Keep existing tests passing. Add tests for new non-visual logic (summary
   text, remembered state, scale bar length) and `qtbot` tests driving the
   real widgets for interactive behavior.
6. End every stage with a report: what changed, files and symbols touched,
   renderer before/after screenshots (inspected), verification as exact
   copy-pasteable commands from the repo root, open questions, and a separate
   “Manual tests” heading for what the owner must do by hand.
7. Codex review at each stopping point. The report for a visible UI change
   includes before/after screenshots and asks for the owner's layout opinion;
   requested changes are new commits.

## Stage order

| Stage | What | Gate |
| --- | --- | --- |
| 0 | Quick fixes | Agent-verified |
| P1 | Live Lab behaviour tests and screenshot scenarios | Agent-verified |
| P2 | Split the RAW and prep panels out of `live_lab_tab.py` | Agent-verified, P1 green before and after |
| P3 | Style tokens, including the objective ring colours | Agent-verified + layout opinion |
| 1–5 | Live Lab pilot | Committed when tests and screenshots prove it; otherwise uncommitted until the owner's manual tests pass |
| 6 | Mål | Layout opinion |
| 7 | Reference picker and create dialog | Layout opinion |
| 8 | Batch correction of microscope settings | **Owner approval before work starts** |
| 9 | Observation workspace (the structural change) | Layout opinion per sub-stage; the owner approves the split first |
| 10 | Sort the remaining dialogs into visibility levels | Owner approves the table |

## Stage 0 — Quick fixes

Scope (each verified in code; locations are starting points):

- Legg til bilder shows two “Lukk” buttons in its footer
  (`ImageImportDialog`: the next/continue button and the cancel button both
  read “Close” when nothing is dirty; `ui/image_import_dialog.py:1291`, `:2579`).
- The Analyse button “Administrer referansebibliotek…” is truncated
  (“inistrer referansebibliot”; `ui/main_window.py:8669`).
- “+ + Ny (N)” shows the plus twice (icon plus the literal in
  `self.tr("+ New (N)")`, `ui/observations_tab.py:2538`).
- English strings in the Norwegian UI. Causes differ: translation equal to the
  English source (“Show thumbnail”, “All except spores”, “Bright cutoff:” in
  `nb_NO`), strings missing from the `.ts` files because they were never
  extracted (“Use for calibration”, “Scan folder”, “Start Session”), and bare
  literals (“Load an image to begin” in `ui/zoomable_image_widget.py:2328`,
  “Preview” at `:124`, “No measurements” drawn by matplotlib at
  `ui/main_window.py:17861`). Also “No measurements”, “Observation”, Live Lab's
  “CURRENT OBSERVATION”, Camera Import's “MICROSCOPE SESSIONS”, “Browse” and
  actions, Reference library's “Close” and missing-metadata warning, and
  thumbnail “From raw” / “Preview” where they show in English.
- The old name “Ingestion Hub” in the offline-session and Sync Shot messages
  (`ui/live_lab_tab.py:7804`, `ui/sync_shot_dialog.py:43`,
  `ui/ingestion_hub_tab.py:1137`); use the current screen name, “Camera
  import” (Kameraimport in `nb_NO`, `sv_SE` and `de_DE`). The “Intestion” typo from the backlog, if still present.
- The English text on the Kameraimport screen (`ui/ingestion_hub_tab.py`).
- Preferanser › Online publishing: “Spør før observasjoner … gjøres offentlige”
  overlaps “Inkluder vannmerke” (`ui/main_window.py:~3417`).
- Referansebibliotek: the publication list's labels wrap and the year column is
  misaligned with its rows (`ui/reference_library_manager_dialog.py`).

Not in stage 0: renaming Sync Shot or the session list (open decision), the
Kameraimport layout reorder (stage 9), and the bin button (needs approval).

Done when: none of these appear in the UI in any of the four languages
(English, `nb_NO`, `sv_SE`, `de_DE`); renderer screenshots of the affected
screens are inspected in `nb_NO` and one other language; existing tests pass.

## Preparation (before the pilot)

### P1 — Live Lab behaviour tests and screenshot scenarios

- `qtbot` tests that drive the real `LiveLabTab` and pin down today's
  behavior the pilot will touch: choosing objective, contrast, mount, stain and
  sample type; the values stored on a newly captured image
  (`_capture_time_metadata`); session start and stop in both session modes;
  session-log events (`_log_session_event`, `_log_dropdown_change`, manual
  notes); RAW controls applied to a pending capture; splitter and section state
  across a restart (persist, destroy, recreate). The camera folder watcher is
  stubbed with files dropped into a temp folder; no hardware.
- Renderer scenarios for Live Lab (idle, session running in each mode, RAW
  capture selected, uncalibrated objective) and Kameraimport (empty, folder
  scanned with matches), in both themes and `nb_NO`, plus a baseline render for
  before/after comparison.
- pytest-qt is already installed in `.venv`; adding it to a test requirements
  file is a separate decision (not a production dependency).

### P2 — Split the RAW and prep panels out of `live_lab_tab.py`

- Move the RAW edit/preview machinery (`PendingRawCapture`, `RawEditSession`,
  preview/commit workers and job types, `RawCurvePreviewWidget`, and the
  `LiveLabTab` methods that only serve them) into its own module, and the
  Microscope, Slide / prep and Notes cards into a panel module. Mechanical
  moves only: no behavior change, facade imports kept, P1 tests green before
  and after, one PR. Do not combine with any redesign.
- Add the new modules to `tools/update_translations.sh`; translation contexts
  change with the class names, so check that no `nb_NO`/`sv_SE`/`de_DE`
  entries are lost.

### P3 — Style tokens and objective ring colours

- Named colour, spacing and radius tokens in `ui/styles.py` for both themes;
  route the global stylesheet through them. Inline colours move to tokens
  screen by screen as each stage touches a screen.
- The objective ring palette defined once, with one function from objective
  to colour that replaces both `_objective_color_for_name` and
  `objective_color`. Defaults by magnification follow ISO 8578:2012, Table 2
  (“Colour of ring”): 1/1.25 black · 1.6/2 grey · 2.5/3.2 brown · 4/5 red ·
  6.3/8 orange · 10/12.5 yellow · 16/20 light green · 25/32 dark green ·
  40/50 light blue · 63/80 dark blue · 100/125/160 white. An objective's own
  stored colour (`color`, `plot_color` and the other keys `objective_color`
  reads) still overrides; macro profiles are neutral grey. In-between values
  (for example 60x) take the nearest standard band; the exact rounding rule is
  tested.
- Today's maps differ from the standard: 16–32x share one green (25/32 should
  be dark green, distinct from 16/20 light green); 1–3.2x, 8x, 12.5x, 80x and
  125–160x fall through to the default blue; 6x is orange only as an exact
  match. Black and white rings need a visible border in both themes.
- The staging orange and the reagent dot colours (`stain_color`) become tokens.
- Verification: renderer scenarios in both themes are identical before and
  after except the intended objective colour changes.

## Pilot: Live Lab (in today's Live Lab tab)

### 1 — Collapsible section widget

- Scope: reusable widget with a header (arrow, title, summary text) and an
  animated content area; collapsed state remembered per section across
  restarts. Apply it to Preparat and Notes in Live Lab, with Preparat as chip
  rows (Medium, Farging, Tilstand, Kilde; click to select, click again to
  clear, “+ N flere” in place).
- Done when: the summary updates immediately when a value changes; the state
  survives a restart; the header toggles with mouse and keyboard; nothing else
  in the layout jumps.

### 2 — Capture-state bar

- Scope: objective and contrast move from the left panel to a floating bar of
  large segmented controls over the live view. A label on the live view
  (“63x · DIC · Vann”) coloured by the objective's ring colour, contrast as
  text. Keyboard shortcuts for objectives and contrast methods.
- Done when: the same colours appear in the bar, the live-view label and the
  thumbnail badges; the old controls are gone from the left panel; the new
  shortcuts don't clash with existing ones. Focus and shortcut behavior on
  macOS needs a cocoa real-input run (`.claude/rules/ui-screenshots.md`).

### 3 — Live scale bar

- Decided 2026-11-09 (owner): Live Lab shows captured images, not a camera
  feed, so the scale bar, the live-view label and the calibration notice
  describe the image on screen (its stored scale and objective; an unsaved
  RAW's objective); with no image shown they follow the selected objective.
  Changing the objective after a capture leaves that capture's label and bar
  alone. This replaces stage 2's label-follows-controls behaviour.
- Scope: a 10 µm scale bar on the live view from the selected objective's
  active calibration and the current zoom; “Ikke kalibrert” with a
  “Kalibrer…” link when there is none.
- Done when: the bar length is correct at several zoom levels (unit test); it
  updates immediately when the objective changes.

### 4 — Contrast memory and settings log

- Scope: switching objective restores the contrast last used with that
  objective (stored as a `SettingsDB` key). Every change of objective,
  contrast, mount or stain is shown as a timestamped line in Notes, reusing the
  existing `session_logs` events rather than a second log.
- Done when: the memory survives a restart; the log lines are saved with the
  session and visible in Notes.

### 5 — Slimmer RAW panel with presets

- Scope: white balance mode, Lys, Mørk and Kontrast stay visible; tone curve,
  its sliders and the histogram go into a collapsed “Mer” section. One preset
  per objective + contrast combination, applied automatically: the RAW
  settings last used with that combination (decided 2026-11-09; today's
  stored settings are keyed by objective, contrast, mount, stain and
  condition and are read as a fallback). No before/after key. RAW panel
  docked by default, pin toggles overlay, choice remembered. “Kopier
  innstillinger” moves from the pending-RAW bar on the image into the RAW
  panel. The mockup's B for before/after is dropped (owner, 2026-11-09): B
  chooses BF since stage 2.
- Owner note 2026-11-09 (white balance with copy/paste and multi-select;
  in stage 5 if it fits, otherwise a follow-up): pasting settings that use
  automatic white balance runs AWB for each target image rather than
  copying the source's multipliers, and copies the source's values only
  where per-image AWB is not possible. The same for a multi-selection: AWB
  per image, or the current image's values where that is not possible.
- Done when: changing contrast switches (or suggests) the preset; manual
  tweaks stay until the next contrast change; the before/after key works.
- Decided 2026-11-09 (owner): the RAW panel stays docked in the side panel;
  no floating overlay or pin (stage 5c dropped).

## Rollout

### 6 — Mål

- Scope: measuring tools (Linje, Rektangel, Flerlinje, start/stop) become a
  toolbar above the image; Skala becomes a summary line; Visning and Info move
  to a right drawer; the left column is removed.
- Done when: the image area is wider; every previous Visning option is still
  reachable; Esc closes the drawer.

### 7 — Reference picker and create dialog

- Scope: picking becomes a drawer over the Analyse plot (from its left edge,
  beside the button; owner 2026-11-09), with search, a
  Bibliotek / Fellesskap filter (Mine observasjoner as a less prominent third
  option), “Kun denne taksonen” on by default, and an expandable row with
  summary and a Detaljer link. Ticking a reference adds it to the plot
  immediately. Creating becomes a two-step dialog: 1) source, 2) paste the
  measurement string and Tolk; the value grid appears only after parsing or on
  “Fyll inn manuelt”. The unlinked-taxon warning appears only when it applies.
- Done when: a reference can be added without leaving Analyse; the plot
  updates while the drawer is open; existing references and the library are
  unchanged.

### 8 — Batch correction of microscope settings (needs the owner's approval)

Data changes are allowed in this stage only, and work does not start until the
owner has answered these:

- Scope: select several images and change objective, contrast, mount or stain.
  Changing the objective recalculates the scale and flags affected
  measurements for review. A confirmation lists how many images and
  measurements are affected. Changes are logged.
- **Schema:** there is no “needs review” flag on `spore_measurements` today.
  Flagging needs a new column or table (a SQLite migration with round-trip
  tests per `database/sqlite_migrations/README.md`). Alternatively flag
  nothing and recalculate silently — which the doc rules out.
- **Recalculation path:** measurements keep pixel points (`p1_x` … `p4_y`)
  and `length_um`/`width_um`. Which of the four existing rescale paths is the
  one to reuse, and are the others retired?
- **Log:** where do batch changes go — `session_logs` (local, not synced), a
  new local audit table (schema), or image `notes`/`lab_metadata`?
- **Sync contract:** images and measurements are cloud-synced. A batch edit
  produces many image and measurement updates; does it go through the normal
  dirty/push path, and does the review flag sync (contract change, sporely-web
  migration, `docs/supabase-sync-contract.md` in both repos)?
- **Cloud-sync rules:** the change touches cloud writes, so
  `.claude/rules/cloud-sync.md` applies; live verification against Supabase is
  human-gated.
- Done when: no measurement changes without being flagged; the confirmation
  shows correct counts; the log records the change.
- Decided 2026-11-10 (owner), from the read-only survey:
  - **Rescale:** one shared database function. It recomputes each
    measurement from its pixel points with the new scale (a multi-line
    measurement from the line stored in its note; a ratio only for rows
    without points), skips calibration rows, updates the image's scale,
    objective and calibration in one transaction, and marks the observation
    for sync. The batch and Mål's objective change use it; the unused
    `CalibrationDB.recalculate_measurements_for_objective` is removed. The
    import dialog, Observations import and calibration-dialog paths move to
    it later. Survey: six writers today (`MainWindow._maybe_rescale_current_image`,
    `_on_calibration_measurement_edited`, `ImageImportDialog._rescale_measurements_for_image`,
    `ObservationsTab._rescale_measurement_lengths`, and the two `CalibrationDB`
    functions); they disagree on points vs ratio and on calibration rows, and
    only the calibration one marks sync. Mål's rescale skips multi-line
    measurements today (they store two points; the line is in `notes`), so
    they keep the old scale.
  - **Log:** one `session_logs` event per observation (images, old → new
    values, measurement count). Local, in backups and exports, not synced;
    no schema change.
  - **Sync:** the new values go through the normal path (`ImageDB.update_image`
    marks the observation dirty; measurements push per row from it). No
    contract change. Checking one batch edit against the real account is
    human-gated.
  - **Review flag:** none (owner, 2026-11-10). Measurements are recomputed
    from their pixel points, so the values are exact; the confirmation with
    counts and the log are the safeguards. No schema change. This replaces
    “flagged” in Done when.

### 9 — Observation workspace (the structural change)

- The pending-RAW bar leaves the image: saving becomes “Lagre til
  observasjon” in the staging strip under the viewer (mockup Bilder-Live).
- Scope: modes in the Observasjoner tab (Vis, Rediger, Bilder, Mål, Analyse)
  with one shared image strip; the list collapses to a rail in Bilder, Mål and
  Analyse with a left drawer; Tabell as a sortable overview sharing data and
  selection with the list; ⌥O, ⌥L, ⌥M, ⌥A map to modes; Live Lab becomes the
  Live source of Bilder (Kamera tilkoblet and Kun logg session types), Legg
  til bilder becomes the Filer og SD-kort source, Mål and Analyse become modes,
  and the top tabs go; Kameraimport stays its own screen, a distinct button at
  the far right of the mode bar (⌥C), with the layout order Import folder,
  camera time offset, sessions, actions. Retire the Rediger observasjon and
  Legg til bilder dialogs. Rediger autosaves (decided 2026-11-10).
- Split into sub-stages that each leave the app usable (proposed below; the
  owner approves the split before 9a starts). A monolith panel in
  `ui/main_window.py` or `ui/observations_tab.py` that a sub-stage must move
  is extracted first, as its own mechanical commit with no behaviour change.
- Decided 2026-11-10: Rediger autosaves; staged images stay with the
  observation they were taken for when switching observation; the
  Kameraimport bin button is left as it is; “Sync Shot” becomes “Camera time
  offset” and “Microscope sessions” “Live lab sessions”. Status og publisering
  changes only `is_draft`; a change that would touch `sharing_scope` or the
  sync contract stops and asks.
- Split (approved by the owner 2026-11-10):

  | Sub-stage | What | Leaves the app |
  | --- | --- | --- |
  | 9a | Mode shell: a mode bar (Vis, Bilder, Mål, Analyse; Kameraimport at the far right, ⌥C) replaces the top tabs; today's pages stay behind it, Live Lab is Bilder for now; Rediger opens today's dialog until 9g; every shortcut keeps working | Same screens, new navigation |
  | 9b | Observation rail: the list (a second view on the table's model; nothing extracted), shown as a rail with a left drawer in Bilder, Mål and Analyse; Tabell from the list header; one selected observation across modes and the table | One list everywhere |
  | 9c | One shared image strip under the modes, replacing Vis's and Mål's strips (Live Lab's joins in 9d, owner 2026-11-10); unsaved images first with an orange border, kept per observation | One strip |
  | 9d | Bilder › Live: captures land in the shared strip (unsaved first, then the observation's images), “Lagre til observasjon” in a row above the strip, the pending-RAW bar leaves the image, unsaved counts in the observation list; a running session keeps its observation (owner 2026-11-10) | Live Lab merged |
  | 9e | Session types (split from 9d, owner 2026-11-10): Kamera tilkoblet and Kun logg, default in Preferanser › Bildeimport, the Kun logg layout (settings bar first, Sync Shot QR at start, running time + Avslutt økt), the Live/Logger badge in the mode bar | Session types |
  | 9f1 | Bilder › Filer og SD-kort, step 1 (owner 2026-11-11: host first, then redesign): a Live / Filer og SD-kort switch; Filer hosts today's Legg til bilder editor in place for Bilder's observation; unsaved work stays with its observation; saves the same way | Import in Bilder |
  | 9f2 | Filer og SD-kort, step 2 (owner 2026-11-11: split the strip off into 9f3): the mockup's layout on the image (Felt/Mikroskop with F/M, the microscope bar only for microscope images, time and GPS on one line with “Bruk for observasjonen”, which also saves the time, owner 2026-11-11; EXIF in an Info drawer; the image tools on the image), Nyquist resizing to Preferanser, off by default; “Velg filer…” beside the source switch | Import on the image |
  | 9f3 | Filer og SD-kort, step 3: the shared strip replaces the editor's own gallery (clicks, multi-select, remove and reorder routed into the editor; unsaved images first, as in Live) | Import redesigned |
  | 9g1 | Rediger, step 1 (owner 2026-11-11: four steps): `ObservationDetailsDialog` moves to its own module, unchanged (mechanical) | Same dialog |
  | 9g2 | Rediger in place for saved observations, today's form layout, with autosave (owner 2026-11-11): fields save about a second after typing stops and when leaving the field or mode; a quiet “Lagrer…/Lagret” status, no Forkast/Lagre and no prompt when leaving; the taxon changes only when a name is picked or an AI suggestion's Bruk is pressed (unpicked text stays, marked as not saved); Utkast, Deling and Posisjon show the publish notice at the change when it makes the observation public or the location more precise (Avbryt puts the control back) | Edit in place |
  | 9g3a | Rediger, the mockup's layout (owner 2026-11-12): Taksonomi and AI-forslag side by side in one panel (AI rows with Valgt / Bruk); Biotop, Substrat and Vokser på stay as tabs in Taksonomi, Art showing the name card with Uidentifisert, Usikker and Fremmed eller dyrket in one row; date, place, and latitude and longitude on one row beside both comments side by side; the map as one line: collapsed it shows the place and country, ▸ opens today's tools in place (Åpne kart, Hent kartlenke), Lim inn kartlenke beside it; the header's status badge (Utkast / Ferdig) | Edit redesigned |
  | 9g3b | Status og publisering (owner 2026-11-12): the stepper Utkast → Ferdig → Publisert til tjeneste from the stored state; Deling and Posisjon beside it (`sharing_scope` stays its own setting, never changed implicitly); the main button “Ferdig og publiser til …” clears `is_draft`, then runs today's Publiser upload for the service chosen with ▾ (Artsobservasjoner, iNaturalist or Both, as in Vis); replaces the Utkast checkbox in Rediger | Publish from Rediger |
  | 9g3c | Rediger uses the shared strip instead of the form's own photo row (owner 2026-11-12); selecting photos there still chooses which ones AI looks at | One strip |
  | 9g4a | Split (owner 2026-11-12). Every Rediger bilde (Vis, Mål, Live, Kameraimport, Rediger) opens Bilder › Filer og SD-kort on the photo, for its observation; the Legg til bilder dialog goes for saved observations | One image editor |
  | 9g4b | Ny observasjon starts in Bilder › Filer og SD-kort for a not-yet-saved observation (owner 2026-11-12): nothing is written before Lagre; Lagre creates it as Utkast with the photos' date and GPS and the default Deling and Posisjon, then Rediger opens; leaving with unsaved photos asks, as Filer does | New in Bilder |
  | 9g4c | Cloud-only rows in Rediger (owner 2026-11-12): the cloud observation with one button, “Importer til denne maskinen”; the import writes only locally and links to the cloud row, as today; then Rediger edits it in place, and its edits sync like any local edit. The Rediger observasjon dialog is retired | Edit merged |
  | 9h | Kameraimport: layout order Import folder, Camera time offset, Live lab sessions, actions; the renames in all three translations; hints updated; bin button unchanged | Stage 9 done |
  | 9i | “Fra SD-kort (nye bilder)” from the mockup (owner 2026-11-11: later, its own stage, after 9h). Decided 2026-11-12: a card is a mounted volume with a DCIM folder (several: ask which; none: say so and offer a folder, remembered); a photo is new when its file name and capture time match no saved image of any observation (Kameraimport's rule); the photos added are those taken on the observation's day (Ny observasjon: the newest day) | SD-card import |
- Done when: the selected observation stays the same across modes and the
  table; unsaved edits are never lost when switching mode or observation; a
  running Live session survives switching mode.

### 10 — Sort the remaining dialogs into visibility levels

The doc covers the dialogs in the table under *Target structure*. This stage
sorts everything else, read-only, into a table the owner approves:

- The 34 dialog classes and 17 inline `QDialog(...)` builds: each gets Docked,
  Collapsed, Drawer or Dialog (the four levels), or *remove* when dead (check
  reachability of `ReferenceAddDialog`, `ReferenceValuesDialog`,
  `CloudReferenceDialog`, `ReferenceLibraryAttachDialog`).
- The 176 `QMessageBox` calls: information becomes a non-blocking notice;
  confirmations stay only for destructive or irreversible actions; cheap
  actions get undo instead. Grouped by screen and kind, not listed one by one.
- `CloudConflictDialog`, OS file pickers and destructive confirmations stay
  real modals.
- Output: the table in this plan, and a proposal for follow-up stages grouped
  by screen. May start any time after stage 5 (it uses the patterns the pilot
  proves); implementation stages come after stage 9.

#### Proposal (2026-11-12, for the owner's approval)

Counted at `063ad84` with an AST pass over `ui/`, `utils/` and `main.py`
(not `tests/`): 36 dialog classes, 20 inline `QDialog(...)` builds and 191
`QMessageBox` calls. “Dead” means no path in the running app; only tests and
the review tooling (render scenarios, the macOS check) use it, so a removal
updates those too. Checked by Codex: the counts reproduce and no dialog is
missing.
Stage 9 already made Rediger observasjon and Legg til bilder in-place pages
(`ObservationDetailsDialog` and `ImageImportDialog` stay as the widgets
Rediger and Filer host; neither opens as a window any more).

**Dialogs by level**

| Level | Dialogs | Why |
| --- | --- | --- |
| Done in stage 9 (in place) | `ObservationDetailsDialog` (Rediger), `ImageImportDialog` (Filer og SD-kort) | Never open as windows now |
| Dialog (stays) | `CalibrationDialog` and `NewObjectiveDialog` (Kalibrer objektiv), `SettingsHubDialog` (Preferanser; it embeds `DatabaseSettingsDialog` and `ArtsobservasjonerSettingsDialog` as pages), `SyncShotDialog` (the time code), `CloudConflictDialog`, `ReferenceLibraryManagerDialog` and its three forms, `SharedReferenceCatalogueDialog`, `MySharedReferencesDialog`, `ReferenceCreateDialog`, `ReferenceAddDialog` (still reached: editing a comparison row), `MicroscopeSettingsDialog` (batch correction), `ScaleBarImportDialog`, `SpeciesPlateDialog`, the three export dialogs, `PortableImportDialog`, `DatabaseBundleOptionsDialog` | Rare setup, data entry or a whole task of its own |
| Dialog (stays, inline) | Destructive confirmations (`dialog_helpers.ask_wrapped_yes_no*`, `ask_measurements_exist_delete`, Kalibrer objektiv's in-use and unsaved-changes questions, possible duplicate observation), credential prompts (Mushroom Observer, Sporely Cloud, Artportalen, Artsobservasjoner web login), cloud sync error details | Destructive or credentials |
| Drawer | Map services (`MapServiceHelper.show_map_service_dialog`): a menu on Rediger's map line, which 9g3a already made one line with ▸ | An occasional task beside its control |
| Merge | The two `_build_cloud_sync_details_dialog` (Preferanser and the main window build nearly the same dialog, 13 and 18 lines) | One builder |
| Check | `show_auto_debug_dialog` (auto-measure traces): developer-only; gate it behind a debug setting or remove | Not for users |
| Remove (dead) | `AddReferenceDialog`'s window and `_on_add_reference_clicked` (its button went in stage 7; the reference drawer still calls its static `_library_group_heading`, `_relevance_badge_text` and `_semantic_badge_text`, which move first, and `EditorScrollArea`, the preview pane and the candidate helpers stay), `_on_reference_panel_plot_clicked` and `_normalize_legacy_reference_for_plot` (no caller), `ReferenceValuesDialog` (`open_reference_values_dialog`), `CloudReferenceDialog` (`_on_reference_panel_cloud_clicked`), `ReferenceLibraryAttachDialog` (`_on_attach_library_reference_clicked`, `_on_manage_reference_library_from_attach`), `_on_reference_panel_add_clicked` and `_on_reference_panel_edit_clicked`, `LanguageSettingsDialog` and `AppearanceDialog` with their openers and `open_database_settings_dialog` (Preferanser has these pages), `open_profile_dialog`, `open_gallery_plot_settings`, `ObservationSelectionDialog`, `LoadingDialog`, `ScaleBarCalibrationDialog`, `RenameObservationDialog`, `CloudSyncDialog` (only tests use it: keep what they cover), `ObservationDetailsDialog._confirm_unfinished_taxon_search` (dialog paths only) | No caller in the app |

**Message boxes by screen and kind** (171 outside dead code; 20 more go with
the removals). Proposed handling per kind:

- *Validation* (“Please enter…”, “Select … first”, “Missing …”): an inline
  message by the field, or the action disabled with its hint saying why.
- *Busy* (“still running, please wait”): the action disabled with a hint.
- *Success / info*: a non-blocking notice. Kept modal: “Restore Complete”
  (it asks for a restart).
- *Stale state* (“The active observation changed while … was open”): a
  notice; most go away once the reference dialogs they guard are drawers.
- *Failure* (with the error): a notice that stays until dismissed, with
  Details. Kept modal: import, restore and archive failures, and “The
  restore could not be rolled back” (the user waits on them; data at stake).
- *Confirmation*: kept for destructive or irreversible actions (delete a
  reference or library item, restore a backup, discard unsaved images or a
  failed Rediger save on quit, reset the cloud account link, remove saved
  passwords, calibration repair, publish notice, stop sharing, rescale
  measurements). Not a question any more: “Update available” (a notice with
  a link) and the cloud download summary (a notice with Details).

| Screen | Validation | Busy | Success / info | Stale state | Failure | Confirmation | Total |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Kalibrer objektiv | 18 | 3 | 1 | 0 | 6 | 2 | 30 |
| Preferanser and logins | 7 | 1 | 5 | 0 | 32 | 3 | 48 |
| References and library | 8 | 0 | 1 | 6 | 23 | 8 | 46 |
| Main window, Vis, Mål, Bilder, Rediger | 5 | 0 | 1 | 3 | 12 | 10 | 31 |
| Import, restore, backup | 0 | 3 | 1 | 0 | 9 | 1 | 14 |
| Cloud sync | 0 | 0 | 0 | 0 | 1 | 1 | 2 |
| All | 38 | 7 | 9 | 9 | 83 | 25 | 171 |

**Follow-up stages (proposed, each its own commit and report)**

| Stage | What | Gate |
| --- | --- | --- |
| 10a | Remove the dead dialogs, handlers and openers above, with their tests and review scenarios, or the coverage moved to live code; no behaviour change | Agent-verified |
| 10b | One shared non-blocking notice (in each screen's hint bar or one app-wide place, the owner's choice), with optional Details and Undo, stays-until-dismissed for errors | Layout opinion |
| 10c | Kalibrer objektiv: validation inline, busy states as disabled actions, success and failures as notices | Layout opinion |
| 10d | Preferanser and logins: validation inline, login and logout failures as notices with Details; the two cloud-sync detail builders merged | Layout opinion |
| 10e | References: stale-state and failures as notices; Referansebibliotek's UUID, citation key and range IDs behind “Tekniske detaljer” (from the target table) | Layout opinion |
| 10f | Main window, Vis, Mål, Bilder, Rediger: “Select an observation first” as disabled actions with hints, image and export failures as notices, “Update available” as a notice; map services as a menu on Rediger's map line | Layout opinion |
| 10g | Import, restore, backup: wording only (they stay modal) | Agent-verified |

Decided 2026-11-12 (owner): the proposal is approved; the notice lives in
each screen's bottom (hint) bar; debug-only code and stale code go
(`show_auto_debug_dialog` turned out to belong to a dead auto-measure path,
`auto_measure_at_click`, which goes with it). 10a is split so each part can
be checked on its own:

| Stage | What |
| --- | --- |
| 10a1 | Dead main-window, settings and misc code: `LanguageSettingsDialog`, `AppearanceDialog` and the standalone openers (Preferanser has those pages; the language fallback tests move to its page), `LoadingDialog`, `ScaleBarCalibrationDialog`, `ObservationSelectionDialog`, `RenameObservationDialog`, `open_profile_dialog`, `open_gallery_plot_settings`, `open_cloud_sync_dialog`, the auto-measure path with its debug window, and other methods in `ui/` that nothing references (checked one by one) |
| 10a2 | Dead reference code: `AddReferenceDialog`'s window (its three helpers move to the drawer first), `ReferenceValuesDialog`, `CloudReferenceDialog`, `ReferenceLibraryAttachDialog` and their handlers, `_on_reference_panel_add/edit/plot_clicked`, `_normalize_legacy_reference_for_plot`; tests that reach live code through them move to the live entry points |
| 10a3 | `CloudSyncDialog` (its OAuth and summary tests move to Preferanser's Sporely Cloud page where it has the same behaviour) and `ObservationDetailsDialog`'s window-only paths (`_confirm_unfinished_taxon_search`; also its genus/species/vernacular text handlers, which only the removed panel's same-named methods kept looking alive) |
| 10a4 | Found in 10a2: the old reference panel's fields (`ref_genus_input`, `ref_species_input`, `ref_vernacular_input`, `ref_source_input`, `ref_table`) are never created since stage 7, yet 20 `MainWindow` methods still read them behind guards (61 references); and the preview stack (`ReferencePreviewPane` with `ReferenceComparisonView`) no longer shows anywhere: the reference editor gets no preview pane and the drawer keeps its pane hidden with its preview off. Untangle both, with the tests that only cover them |
| 10a5 | Found in 10a3: the form is never a window any more, yet keeps its window mode: Save and Cancel (hidden in place), `accept`/`reject`'s window branches, the window geometry, Enter to save and the hints for those buttons. The publish-notice tests drive `accept()` as their harness, so they move to `confirm_publish_change` first. Also the hidden backing fields' completer plumbing: the main genus, species and vernacular fields are hidden since the taxon picker, yet keep completers, placeholders (`_vernacular_placeholder`) and `_on_vernacular_editing_finished_populate_genus` on `editingFinished`, which a hidden field never emits; `TaxonPicker.set_ai_suggestion_handler` has no caller (the host fields stay: they are visible) |
| 10a6 | Found in 10a5: the typed-name Red List match. `_on_taxon_manual_editing_finished` hooks the hidden genus and species fields' `editingFinished`, which never fires since the taxon picker, so `TaxonInputController.set_display_only_name_match` has no live caller and the form's two readers of `display_only_name_match()` (the Red List staleness token and the deferred resolve) always get nothing. Remove the hook, the controller's display-only match and those readers together. The Stage 3B.5 invariant tests built on a copy of the hook (`_Host` in `tests/test_red_list_identity_invariants.py`) and the badge-regression tests that call it go, or move to what the picker does. Red List identity code: a Codex challenge review |
| 10d2 | Split from 10d (10d1 did Preferanser's own pages): the publishing and login pages' boxes in `ArtsobservasjonerSettingsDialog` (24: Sporely Cloud sign-in, sign-out and account reset, the web logins for Artsobservasjoner and Artportalen, Mushroom Observer, removing saved passwords) and the credential prompts' validation in `utils/artportalen_auth.py` and `utils/artsobservasjoner_auto_login.py` (6). Prompts and confirmations stay dialogs; their validation goes inline; failures become notices with Details (the Sporely Cloud card's on the Profile page's bar). Login and session logic unchanged; it is account-linking code, so a Codex challenge review |

## Decisions (2026-11-08, answers to the first round of questions)

- **Mockups and screenshots** stay local in the gitignored folder; referenced
  by absolute path; nothing from it is committed.
- **Commit rule:** see *Workflow*.
- **English source names** (proposed by the agent at the owner's request; strings go
  through `tr()` with these English sources and the doc's Norwegian as the
  `nb_NO` translation):

  | `nb_NO` (doc) | English source |
  | --- | --- |
  | Vis / Rediger / Bilder / Mål / Analyse | View / Edit / Images / Measure / Analysis |
  | Tabell | Table |
  | Kameraimport · alle observasjoner | Camera import · all observations |
  | Live (mikroskop) / Filer og SD-kort | Live (microscope) / Files and SD card |
  | Kamera tilkoblet / Kun logg | Camera connected / Log only |
  | Live / Logger (mode-bar badge) | Live / Logging |
  | Avslutt økt | End session |
  | Lagre til observasjon | Save to observation |
  | Preparat: Medium, Farging, Tilstand, Kilde | Preparation: Mount, Stain, Condition, Source |
  | + N flere | + N more |
  | Notater | Notes |
  | Ikke kalibrert · Kalibrer… | Not calibrated · Calibrate… |
  | fra kamera | from camera |
  | Felt / Mikroskop | Field / Microscope |
  | Bruk for observasjonen | Use for observation |
  | Status og publisering: Utkast → Ferdig → Publisert til tjeneste | Status and publishing: Draft → Finished → Published to service |
  | Ferdig og publiser til … | Finish and publish to … |
  | Tekniske detaljer | Technical details |

  Existing English sources that already mean the same thing are reused
  (“Measure”, “Analysis”, “Camera import”, “Notes”, “Mount:”, “Stain:”).
- **Kameraimport bin button** (owner, 2026-11-10): leave it as it is; the
  proposed hide-and-confirm design is not built.
- **Kameraimport placement:** a distinct button at the far right of the mode
  bar, ⌥C.
- **Shortcuts:** keep ⌥I (Camera import from Observations) and ⌘E/⌥E (Edit)
  as aliases. Single-letter keys act only when no text field has focus.
- **Objective colours:** the ring standard sets each objective's default
  colour by magnification; the per-objective override stays; macro is neutral
  grey. The code's 25–32x green was checked against ISO 8578:2012 Table 2: the
  standard has dark green for 25/32 and light green for 16/20, so the shared
  green is a deviation P3 fixes.
- **Status og publisering:** Ferdig only clears the draft flag; visibility is
  never changed implicitly.
- **Stage 2 label** is coloured by objective; **Preparat** has no “–” option;
  **AI-forslag** sits inline in Rediger.
- **Rediger save model** (owner, 2026-11-10): autosave; no Lagre/Avbryt prompt
  when leaving.
- **Staged images when switching observation** (owner, 2026-11-10): they stay
  staged for the observation they were taken for, with no prompt and no
  automatic save; the rail shows a count on that observation.
- **Naming** (owner, 2026-11-10): “Sync Shot” becomes “Camera time offset”
  and “Microscope sessions” “Live lab sessions” (stage 9h).

## Open decisions (settle before the stage named)

- [x] Decided 2026-11-09: 1–6 choose the objectives in magnification order,
      B/D/I/P the contrast methods BF/DF/DIC/Phase; they act only when no text
      field has focus. P also means “previous image” in Measure: revisit when
      stage 9 merges the screens.
- [x] Decided 2026-11-09: changing objective or contrast applies, by default,
      the RAW settings last used with that objective + contrast combination.
- [x] Decided 2026-11-09: no hold-to-compare key for RAW before/after in
      stage 5.
- [x] Decided 2026-11-09 (stage 7): in the reference drawer, ticking a
      reference attaches it to the observation and plots it; unticking
      detaches it (the same as Fjern in the comparison list). A ticked
      reference is attached as “Sammenlignet”; the role is changed in its
      expanded row.
- [x] Decided 2026-11-10: Rediger autosaves.
- [x] Decided 2026-11-10: switching observation keeps unsaved images staged
      for the observation they were taken for.
- [x] Decided 2026-11-10: the Kameraimport bin button stays as it is.
- [x] Decided 2026-11-10: “Camera time offset” and “Live lab sessions”.
- [x] Decided 2026-11-08: Analyse and Live Lab are modes in Observasjoner;
      Live Lab and Legg til bilder merge into the Bilder mode.

## Background for the open decisions

- **Kameraimport bin button** (`_delete_sessions_for_observation`,
  `ui/ingestion_hub_tab.py:1771`): deletes all `offline` session-log events for
  an observation immediately, with no confirmation. `session_logs` is not
  cloud-synced (no reference in `utils/cloud_sync*.py`) but is part of the
  portable archive, so deleted logs also drop out of later exports.
- **Sync Shot naming:** the backlog asked to rename “Sync shot” to “Camera time
  offset” and “Microscope sessions” to “Live lab sessions”; the doc and the
  mockups keep “Sync Shot”.

## Manual tests after the pilot (on a real microscopy session)

- [ ] Switch objective and contrast a few times. Could you tell the current
      setting without looking for it?
- [ ] Leave the wrong objective selected on purpose. Did the scale bar make it
      obvious before you captured?
- [ ] Count how often you still had to scroll the left panel.
- [ ] Note any collapsed section you opened only to read a value. That value
      is missing from its summary line.

## Screen backlog (from INBOX, 2026-11-08)

Items moved here from `INBOX.md`. Fix each in the stage that rebuilds its
screen unless it blocks daily use. Screenshot defects moved to stage 0.

### Analyse (stage 7)
- *feature* Default “Orient” and “Uniform scale” to on.
- *feature* Cmd/Ctrl-click additive selection and histogram additive selection.
- *feature* Consider renaming “Reference shape” to “Shape”.
- *feature* Make cloud-origin imported reference sources more visually
  distinct in the reference panel.

### Mål (stage 6)
- *bug* Make room for the measure-type radio-button labels.
- *feature* Hint bar at the bottom of Measure.
- *feature* Fine-tuning for multi-line measurement segments.
- *bug* Hide the “stain” pill on field images in Measure and observation
  main-image surfaces. (Measure: fixed in `5de74ec`.)
- *bug* Local database values must take priority over file EXIF in the Measure
  Info box (verify first; same check for Legg til bilder).

### Observasjoner and main window (stage 9)
- *bug* Selection/highlight artifacts in AI suggestions and the Observations
  table; align them with the Measurements table.
- *feature* The main window still uses the four-field `TaxonInputController`
  (`ui/main_window.py`, also `ui/cloud_reference_dialog.py`); the observation
  editor uses the unified taxon picker (PR #47). Decide whether these move to
  the unified picker (`docs/plans/completed/2026-10-27-unified-taxon-picker.md`).
- *bug* Quitting after an aborted sync crashed with `QThread: Destroyed while
  thread 'Artsobs mobile link check' is still running` (2026-10-19). A guard
  exists near that worker in `ui/observations_tab.py`
  (test in `tests/test_observations_tab_cloud_sync.py`); this path escapes it.

### Legg til bilder → Bilder, Filer og SD-kort (stage 9)
- *bug* Editing a raw image and then zooming can revert the preview to the old JPEG.
- *bug* Thumbnails do not update after edits.
- *bug* The cloud-synced image warning overlay is wrong.
- *bug* Android-imported JPEG portrait rotation in thumbnails (and Measure).
- *feature* Adjustable thumbnail gallery height; no cropped/hidden
  thumbnails; thumbnails around 100 px.
- *feature* Multi-select auto white balance, calculated per image.
- *feature* Apply one custom/picked white balance to all selected images.

### Live Lab (pilot)
- *bug* After editing a “from raw” image, the main preview stays stale until
  another image is selected (stage 5).

### Kameraimport (stage 9)
- *feature* Layout order: Import folder, camera time offset, sessions,
  Actions; update button hints (renamed “Camera time offset”, decided
  2026-11-10).
- *feature* Richer manual reassignment for unmatched images (also in
  `docs/hardware-sync.md`).

### Species plate / publishing (stage 10)
- *bug* The publication plate must respect disabled image bubbles.

### Cloud and account surfaces (stage 10)
- *bug* Replace “Unable to save cloud login” for account-link protection
  failures with wording that names the account mismatch.
- *bug* The account-mismatch message tells users to reset the cloud link,
  but no such tool exists: add a real Reset Cloud Link tool or remove the
  instruction. (A real reset tool touches account binding: security review.)
- *feature* A non-modal pending-cloud-media indicator on close; no blocking dialog.

### Other, not tied to a stage
- *feature* Menu link to Pro information/payment on `sporely.no`; no embedded checkout.
- *feature, deferred* Observer-safe `Help → Taxonomy` menu
  (`database/taxonomy/docs/ui-menu-recommendation.md`).
- *feature, proposed* “Search in Species Fungorum” action from the taxon
  detail UI; investigate a reliable deep link first (full notes in git
  history of `INBOX.md`, 2026-10-24).
- *later* Shared macOS real-input helper from
  `tools/review_ui/taxon_picker_mac_check.py`, when a stage first needs a
  cocoa run (stage 2 shortcuts or the Preparat chips).
- *later* Renderer speed: `test_default_renderer_emits_central_manifest_and_all_established_states`
  times out at 120 s and is excluded from CI.

## Progress

Record each stage as it lands: its commit on `ui/redesign` (stages 0 and P1
landed through PRs before the branch existed), the commit that brought it into
`main`, verification tier, manual test status.

| Stage | Commit or PR | On `main` | Status |
| --- | --- | --- | --- |
| 0 | sigmundas/sporely#65 | `a682f6c` | Agent-verified (tests, inspected renderer screenshots); owner tested it 2026-11-08 and said to go on; merged 2026-11-09 |
| P1 | sigmundas/sporely#67 | `3162501` | Agent-verified (tests, inspected renderer screenshots); no production code; merged 2026-11-09 |
| P2 | `adb561f` | — | Agent-verified: Live Lab, RAW and main-window tests green; `live-lab` renders pixel-identical to the P1 baseline in both themes; translations moved to the new contexts with none lost |
| P3 | `e279277` | — | Agent-verified: the global stylesheet, design tokens and button icon colours are byte-identical in both themes; ring colour, outline and token tests; renders identical except the intended objective colour changes; Codex review: 3 findings fixed (Measure tag colour from the profile, NA-first names, outlined badge size). Owner's layout opinion 2026-11-09: looks good |
| 1a | `c03845e` | — | Agent-verified: collapsible Preparation and Notes sections (summary line, warning for unset values, open state remembered); qtbot tests for mouse, keyboard, restart, summaries and layout; renders change only Live Lab's left panel; Codex review: 5 findings, 4 fixed, 1 not reproducible but guarded. |
| 1b | `e52bc3b` | — | Agent-verified: Preparation rows as chips (click to choose, click again to clear, “+ N more” in place; arrows move, Space/Enter choose; Norwegian labels Medium, Farging, Tilstand, Kilde); qtbot tests; renders change only Live Lab's Preparation rows; Codex review: 3 bugs fixed (idle layout loop, arrow keys clearing a chip, over-wide chip), 1 test gap closed. Stage 1 done; owner's layout opinion 2026-11-09: looks good |
| 2 | `194b4f6` | — | Agent-verified: capture-state bar (objective and contrast with ring colours, 1–6 and B/D/I/P shortcuts), live-view label, Microscope card removed; the P1 objective-restore bug fixed; qtbot tests; cocoa real-input run 20/20 (`tools/review_ui/live_lab_mac_check.py`); renders change only Live Lab; Codex review: 6 findings, 5 fixed, 1 kept as designed. Owner's layout opinion 2026-11-09: looks good; the pending-RAW bar should leave the image (stages 5 and 9) |
| 3 | `0d7b2b0` | — | Agent-verified: the scale bar, live-view label and a new “Ikke kalibrert · {objektiv}  Kalibrer…” notice describe what the viewer shows (owner's decision 2026-11-09); bar length tested at five zoom levels; renders change only Live Lab's uncalibrated states; Codex review: 7 findings fixed. Owner's layout opinion 2026-11-09: looks good |
| 4 | `17f83a8` | — | Agent-verified: switching objective brings back its last contrast (SettingsDB `live_lab_contrast_by_objective`, only while preparing the next capture); Notater lists the session's notes and objective, contrast, mount and stain changes, newest first, from `session_logs`; qtbot tests incl. restart and a real gallery selection; cocoa run 20/20; Codex review: 4 findings fixed, 1 test gap closed |
| 5 | `e7dfb9f`, `7323515`, `6d7c802`, `f5cad5a` | — | Agent-verified: (a) RAW settings follow objective + contrast, stored per combination with the old key as fallback, saved captures keep their own combination; (b) RAW panel slimmed (white balance, Lys, Mørk, Kontrast; levels, tone curve and histogram under “Mer”), copy/paste settings moved into the panel; (c) dropped: the owner keeps the RAW panel in the side panel (2026-11-09); (d) pasted settings carry white balance per image. qtbot tests; Codex review: 4 findings fixed, 1 test gap closed. Owner's layout opinion 2026-11-09: looks good; the Live Lab pilot is complete |
| 5e | `d51ab4b` | — | Owner's first session (2026-11-09), fixed: after switching contrast back (BF → DIC) the RAW panel still showed the BF image on screen, so DIC's settings seemed lost. Now an objective or contrast change shows that combination's settings and edits them for the next capture; a capture shown or an image chosen takes the panel back; Pick is off meanwhile. Notater leaves objective and contrast changes out in live sessions (still in `session_logs`; offline sessions list them). Start/Stop økt moved into Gjeldende observasjon. qtbot tests (the switch-back test fails on the previous code); renders change only the observation card and the label position; Codex review: 2 findings fixed (late preview, Pick) |
| 5f | `5f0221e` | — | Owner 2026-11-09: no session log in live Live Lab; the timestamped note is for offline microscopy, notes go per image and show in Mål. Live sessions: Notater shows only Bildenotat (the session log is still written; offline unchanged). Bildenotat is now stored as the image's `notes` (Mål's “Notat per bilde”); before, a saved image's note was lost and a RAW's stayed in lab metadata. Live Lab and Mål refresh each other's editor for the same image. qtbot tests; renders change only Notater (live and idle) and move the RAW panel up; Codex review: 1 finding fixed (stale editors) |
| 6 | `6a04fce`, `abf1810` | — | Agent-verified: Mål has no left column; Skala is a collapsed section with a summary line (objective · calibration, scale warning as alert; SettingsDB `measure_scale_section_expanded`); category, Linje/Rektangel/Flerlinje, Start/Stopp måling and the status are a toolbar above the image; Visning and Info are pages of a new shared right drawer (`ui/side_drawer.py`) that overlays the image, closes with Esc, its ✕ or its button, and widens to fit its page; Info shows a dot when the image has a note. Splitter key `measure_main_splitter_sizes_v2`. New renderer scenarios `measure.tab*`, `measure.view-drawer*`, `measure.info-drawer*` build a real MainWindow; qtbot tests in `tests/test_measure_layout.py`. Codex review: 2 findings fixed (toolbar overlap in a narrow pane, drawer too narrow for a large font). Owner's layout opinion 2026-11-09: looks good |
| 7a, 7b | `b2adcd7`, `a9c818f` | — | Agent-verified: (a) renderer scenarios `analysis.*` build a real MainWindow; (b) “Legg til referanse…” opens a right drawer over the plot: search, Bibliotek / Fellesskap, a quieter “Mine observasjoner”, “Kun denne taksonen” on by default; ticking attaches as Sammenlignet and plots, unticking detaches; the expanded row has the summary, the role (changed in place) and “Detaljer…” (opens the library manager on that set). Attachments are read from the database for the active observation. Fellesskap, Mine observasjoner and Ny referanse still open the old dialog on that tab (7c, 7d). qtbot tests in `tests/test_reference_drawer.py`; Codex challenge review: 6 findings fixed with regression tests |
| 7c | `a00adbd`, `0c5d9f5` | — | Owner 2026-11-09: the drawer was far from its button; it now opens at the plot's left edge beside “Legg til referanse…” (drawer rule: next to what opened them). Agent-verified: Fellesskap and Mine observasjoner tick like the Library (plot series, not attachments); Fellesskap loads a dataset's detail from Sporely Cloud on tick, then plots the summary or raw points; ticks follow the comparison list and observation switches. Only “Ny referanse…” still opens the old dialog (7d). qtbot tests (26 in `tests/test_reference_drawer.py`); Codex challenge review: 6 findings fixed with regression tests |
| 7d | `f4813f8` | — | Agent-verified: “Ny referanse…” opens a two-step dialog (`ui/reference_create_dialog.py`, the shared `ReferenceEntryEditor` in a staged mode): 1) Kilde (publication, navn som publisert, lokator), 2) Målinger (paste, Tolk; the grid appears after a successful parse or “Fyll inn manuelt”; Bytt L↔B and Korriger tolkning with it). Lagre i bibliotek stores without plotting, Legg til i diagram stores and attaches; both close on success and the drawer reloads. The unlinked-taxon notice shows only without a linked taxon and points to Rediger. Agent's call: no “use an existing set” choice in the dialog (the drawer does that); step 1 says how many sets the publication already has for the taxon. Also fixed: switching publication in the editor lost the selection. Old one-column editor renders unchanged. qtbot tests (13 in `tests/test_reference_create_dialog.py`); renderer `analysis.create-reference-*`; Codex review: 4 findings fixed (typed publication name, Save for unstorable values, two taxon-drift gaps) plus Enter in the paste field adding the reference. Owner's layout opinion 2026-11-10: looks good; stage 7 done |
| 8a, 8b | `beb2897`, `e9dab37` | — | Agent-verified: (a) one rescale path, `MeasurementDB.apply_scale_changes` (database/models.py): recomputed from the points the way each measurement was made, by the ratio when there are none or they no longer give the stored length (a resized image's polyline, a pulled multi-line row); calibration rows left alone; image scale, objective and calibration in one transaction; only a real change marks the observation for sync. Mål's objective and custom-scale change use it, so single lines and multi-line measurements are now rescaled too. (b) Mål gallery, right-click → “Endre mikroskopinnstillinger…”: objective, contrast, medium, fargestoff; preview of images changed and measurements recalculated, Apply writes exactly that and refuses if the images changed; `MicroscopeSettingsDB` in one transaction, microscope images only, objective scale divided by the resize factor; one `session_logs` event per observation of kind “edit” (never read by the offline matcher), images by sync key with old and new values. qtbot tests in `tests/test_scale_change.py` (12) and `tests/test_microscope_settings_batch.py` (13); renderer `measure.microscope-settings*`; Codex challenge reviews: 8a 5 findings fixed, 8b 7 fixed. Human-gated, not done: sync one batch edit against the real account and check it on web |
| 8 fixes | `5de74ec` | — | Owner's test 2026-11-10 (observation 991): changed values reach the web. Found and fixed: an objective change compared against Mål's in-memory scale, which can be stale, and skipped the recalculation while writing the new scale (image #8 kept 100X measurements under 63X); it now compares with the stored scale. Thumbnails did not show a changed objective (the gallery rebuilt only for a new id or file). A field image showed the previous image's objective and “Ingen farging”. The scale bar now stays in the view's lower right corner when zoomed and steps down to a round length that fits. qtbot tests in `tests/test_measure_scale_state.py`; Codex review: 3 findings fixed (bar label, multi-selection, strip scroll). Image #8 repaired 2026-11-10 with the owner's approval: its 14 measurements recalculated from their points at 63X in the local database (backup taken first); every microscope image in observation 991 now matches its scale; the corrected values reach the web on the owner's next sync. Open: the batch sync check with the dialog |
| 9a | `8874a1d` | — | Agent-verified: a mode bar (`ui/mode_bar.py`) replaces the top tabs: Vis, Rediger, Bilder, Mål, Analyse with their ⌥ keys on the left; Kameraimport · alle observasjoner (camera icon, count of matched photos from the last scan still waiting) and the corner buttons on the right. The pages stay in `tab_widget` with its tab bar hidden, so every existing caller and ⌥ key keeps working; ⌥C now also opens Kameraimport from Observasjoner (⌥I stays). Rediger opens today's editor for the observation shown: the list selection in Vis, the active observation on the other pages. Bilder is Live Lab for now; its recording dot moved from the tab to Bilder. Kameraimport shortens to its name when the bar is tight; mode names never shrink; the window's minimum width is unchanged (967/1014/968 px in en/de/nb). qtbot tests in `tests/test_mode_bar.py` (22); renderer `mode-bar.*`; Codex review: 6 findings fixed (Edit on another page edited the list's selection, Edit stale after filtering, excluded photos in the count, label fit after a corner change and with fixed gaps, the recording dot) |
| 9b | `74f41f6` | — | Agent-verified: in Bilder, Mål and Analyse a thin “OBSERVASJONER ▸” rail opens the observation list as a left drawer (`ui/observation_list_panel.py`): + Ny, Tabell, search and one line per observation (number, then the common name or the scientific one). Agent's call, instead of first extracting the list from `ObservationsTab`: the list is a second view on the table's own model, so order, search, cloud rows and thumbnails mode are the table's and no code moved. Choosing a row (click or Enter) opens it on the page shown and closes the drawer; Tabell goes to Vis, whose table is the Tabell overview for now (the docked list in Vis and Rediger comes with 9f). One selected observation: the table follows the observation shown on every page, except a batch chosen in Vis; Vis decodes its full image only when shown. `SideDrawer` gained an opt-in `escape_anywhere` (Esc anywhere in the window closes it before a page's own Esc); drawers open one at a time. qtbot tests in `tests/test_observation_list.py` (13); renderer `observation-list.*`; Codex review: 5 findings fixed (a batch collapsed by a background activation, the hidden View image decoded, numbers lost with table thumbnails, a hidden Enter target, Esc from the page) |
| 9c | `b7c9caa` | — | Agent-verified: Vis and Mål share one image strip (`ObservationsTab.gallery_widget`; `measure_gallery` is the same widget). It moves into a slot on the page shown and keeps selection, scroll and height; its signals reach only that page (`GalleryChannel`), and each page keeps its right-click actions (Vis: move, delete cloud copy; Mål: change microscope settings). A page that does not hold the strip never writes to it and reloads it when it takes it back. The image chosen in Vis's strip opens in Mål; a multi-selection made in Vis stays. The strip can show unsaved images per observation first (`set_staged_items`): orange border, no cloud checkbox, never passed to delete/move/change settings, never reordered among saved ones; Live Lab feeds it in 9d. Owner's decision 2026-11-10: Live Lab's own strip joins in 9d (it shows the session's captures, not the observation's images). Also fixed: `select_image` left the rest of a multi-selection highlighted. qtbot tests in `tests/test_shared_image_strip.py` (13); renderer `image-strip.staged*`; Codex review: 4 findings fixed (View re-selecting while Mål held the strip, multi-selection collapsed on entering Mål, staged images dragged among saved ones, a file dropped on the strip in Mål starting a new observation) |
| 9d | `11dc53b` | — | Agent-verified: Bilder (Live Lab) holds the strip shared with Vis and Mål: the session observation's images with its unsaved captures first, orange (`set_staged_items`, per observation). Saving is a row above the strip (“N ikke lagret · Lagre gjeldende · Lagre til observasjon”); the bar on the image is gone. While another page holds the strip Live Lab writes nothing to it and reads only its own selection (`_LiveAwayStrip`), so capture-bar and RAW edits never reach that page's selection; Bilder takes the strip back when shown. Captures stay with the observation they were taken for; a running session keeps showing its observation when another is opened elsewhere, with “Øktens observasjon: bildene lagres her” on its card (owner 2026-11-10). The observation list shows unsaved counts. Images saved before the session are view-only in Bilder (the capture bar keeps editing the next capture; delete says to use Vis or Mål). Owner 2026-11-10: 9d split; session types are 9e. qtbot tests in `tests/test_live_staging_strip.py` (12, real RAW captures through the harness watcher); renderer `images.live*`; Codex review: 6 findings fixed (multi-selection collapsed on refresh, stale edit targets after a capture while away, a background save collapsing Mål's selection, the save button's label, Bilder not following a new observation, earlier images silently not deleted) plus a crash it exposed (publish state for unsaved ids) |
| 9e | `c502b70` | — | Agent-verified: the observation card in Bilder chooses the session type, Kamera tilkoblet or Kun logg (agent's call: the choice is remembered and is the same setting as the default in Preferanser › Bildeimport, whose labels now use these names), shows the watched folder with “Endre…” for Kamera tilkoblet (owner 2026-11-10: the folder was hard to find), and while a session runs its time (“Live · 00:14:22” / “Logger · …”) and “Avslutt økt”. A Kun logg session replaces the viewer with a page: the settings first (summary, “Gjelder bilder tatt fra HH:MM”, the objective and contrast bar moved onto the page and fitted to its width), the camera-time QR (`SyncShotQrView`, the same code as the Sync Shot dialog, with “Fullskjerm”) and “Etterpå” steps describing today's Kameraimport; a photo chosen in the strip still shows, with “Tilbake til loggen”. The mode bar shows “Live 00:14” / “Logger 00:14” on Bilder. Session status texts use the new names. qtbot tests in `tests/test_session_types.py` (8); renders `live-lab.*` and `images.live*`; Codex review: 3 findings fixed (a photo hidden behind the log page, the bar not fitting the page, a stale summary) |
| 9f1 | `11a814b`, `068062d` | — | Agent-verified: (a) mechanical: `ObservationsTab.save_prepared_images` extracted from `open_edit_images_direct` (AST-identical body). (b) Bilder switches between “Live (mikroskop)” and “Filer og SD-kort”; Filer hosts today's Legg til bilder editor (`ImageImportDialog.set_in_place`) as a page for Bilder's observation (`ui/files_source_page.py`), in a scroll area, with a save row that stays in view (“N ikke lagret · Forkast endringer · Lagre til observasjon”); Esc keeps the work. Unsaved work stays with its observation across switches; its new images show as unsaved in the shared strip (staged items now kept per source, so Live and Files never replace each other) and in the list's counts; quitting with unsaved work asks. Saving writes only what the editor changed: saved images it did not touch are re-read from the database first, images added elsewhere are kept, the GPS is written only if changed there, the table selection stays; a clean editor is rebuilt when shown again. The save path's fallback re-select now finds the row by observation (it used the position in `get_all_observations()`). Owner 2026-11-11: host first, redesign in 9f2; the dialog stays for Ny observasjon, Rediger and Rediger bilde until 9g. qtbot tests in `tests/test_files_source.py` (11); renderer `images.files*`; Codex review: 6 findings fixed (edits that mark nothing dirty, stale overwrite, GPS revert, wrong-row activation, window geometry, a second click while saving), 2 noted below. One unexplained abort in one broad run, not reproduced in two full and five targeted runs |
| 9f2 | `965a76a` | — | Agent-verified: in place only (the dialog is unchanged until 9g), the Legg til bilder editor puts its per-image controls on the image (`ui/image_hud.py`, `ImageImportDialog._apply_in_place_layout`): Felt/Mikroskop with F/M at the top left (for all selected; neither checked for a mixed selection), the selected field photo's time and GPS on one line with “Bruk for observasjonen” (“Brukt for observasjonen” when they are the observation's already), Info at the top right opening the photo's EXIF in a drawer (only when it has EXIF; Esc closes it), Beskjær/AI-beskjæring/Roter as a bar at the bottom, and Live Lab's objective and contrast bar (`CaptureStateBar` on the editor's own selectors) above it, for microscope images only. The details panel (EXIF card, observation Tid og GPS, Endre størrelse og beskjær) goes; Preparat shows for microscope images only; the dialog's 1200 × 800 minimum no longer makes the page scroll. “Bruk for observasjonen” also saves the observation's time (owner 2026-11-11): only when pressed, the photo's time as chosen, never a time the editor seeded or round-tripped. Optimal-sampling resizing moved to Preferanser › Bildeimport › Filer og SD-kort (off by default; an open editor follows a change). “Velg filer…” beside Bilder's source switch. qtbot tests in `tests/test_files_layout.py` (11, real clicks); renders `images.files*`, `images.preferences-image-import*`; Codex review: 7 findings fixed (a seeded time, a timezone round-trip and an undated observation could write the wrong time or none; “Bruk” disabled when only the minute matched; Esc and the Info drawer; an open editor ignoring the preference; Preparat showing at first for a field photo). The renderer test that renders every scenario now exceeds its 120 s limit locally (CI skips that file); its group list lacked `mode-bar` since 9a, fixed |
| 9f3 | `8414a1d` | — | Agent-verified: while Bilder shows Filer og SD-kort, the strip shared with Vis, Mål and Live replaces the editor's own gallery (`FilesSourcePage.claim_shared_strip`, channel `files`; `ImageImportDialog.attach_strip` puts it in the gallery's place under the image, at its height). The editor stays the source of truth and renders its images there, new ones first with the orange border (saved last, in their order); the strip's clicks, Ctrl-clicks, Edit, removal (× and Delete) and reordering reach the editor shown. The strip follows the source (Live ↔ Filer), the page (Vis/Mål take it back and reload), the observation (each observation's editor) and a save or discard (the fresh editor); the selection goes with it in both directions. Also: the Skala card shows for microscope images only (owner 2026-11-11, `d717af4`); Live's page-wide keys (RAW review arrows/Delete/Enter/Esc, objectives 1–6, contrast B/D/I/P) are off while Filer shows — since 9f1 they could act on Live's captures from Filer and made the editor's Delete ambiguous; thumbnail labels (Felt, Mikro, Skalastrek, Fra raw, mangler skala) now translate on every page (they were filed under no context and showed in English); cloud checkboxes show only where they are saved (Vis, Mål), not in Live (since 9d a click there was lost) or Filer. qtbot tests in `tests/test_files_strip.py` (11, real clicks and keys; reordering through the strip's drop handler), `tests/test_files_layout.py` (+Skala); renders `images.files*`, `images.live`, `image-strip.staged`; Codex review: 2 findings fixed (cloud checkboxes in Filer saved nothing; a selection made in code was lost or wrong in the strip) |
| 9g1 | `8ae7a4d` | — | Agent-verified, mechanical: `ObservationDetailsDialog` moved verbatim from `ui/observations_tab.py` (−7.8k lines) to `ui/observation_details_dialog.py` (class AST identical; the rest of `observations_tab.py` identical but for a re-export block at its end, guarded for importing the new module first). The helpers it shares with the Observations tab stay there and are imported from it. Tests and the review scenario that patched `observations_tab` globals the dialog reads (`resolve_vernacular_db_path`, `AIGuessWorker`, `INatAIGuessWorker`, `load_objectives`, `_current_utc_timestamp_text`, `list_available_vernacular_languages`) patch both modules. `tools/update_translations.sh` scans the new file (the `.ts` files change only in `<location>` lines). Owner 2026-11-11: 9g in four steps; autosave, the publish notice at the change, a quiet “Lagret” status, and only a picked name saves the taxon (see the split above). 1431 tests in the 60 files that touch the dialog or `observations_tab` pass; renders `observations.taxon-picker-card`, `observations.taxon-link-offer` unchanged; Codex review: no findings |
| 9g2 | `1d69a1e`, `c0cfdc9` | — | Agent-verified: (a) mechanical: `ObservationsTab.write_observation_edits` (edit_observation's write of the observation fields) and `ObservationDetailsDialog.confirm_publish_change` (accept's publish notice and precision record) extracted. (b) Edit in the mode bar (and ⌘E/⌥E, the table's Edit) shows the form in place as the Rediger mode (`ui/edit_page.py`, tab index 5, appended; the rail as in Bilder) for a local observation; cloud-only rows keep the dialog until 9g4. Autosave (owner 2026-11-11): about a second after typing stops and at once when leaving the field, the observation or the mode; “Lagrer…/Lagret” where Save was; no Save, Cancel or prompt; Esc and Enter keep the page. Saved is every change to what the form would save once it has opened, whatever made it (as Save did: a place name looked up for a new position saves too), and only the changed columns (a picked name and a moved point as a whole), so values changed elsewhere meanwhile are kept; opening and browsing write nothing, the AI state included. Only a picked name (or an AI suggestion's Bruk) changes the taxon; typed search text is never saved and the status says so. Utkast, Deling and Posisjon ask the publish notice at the change, judging the stored row as it will be saved; Cancel writes nothing about publishing. A failed save stays pending and says so; leaving asks Prøv igjen / Forkast den, quitting asks too and waits while the notice is open. A taxon change reloads the form's image paths; showing Rediger again rebuilds the form from the database; a deleted observation empties it. Images are not changed here: Rediger bilder opens Bilder › Filer og SD-kort on that image. qtbot tests in `tests/test_edit_mode.py` (37, real clicks, keys and wheel; modal notices as nested event loops); renders `edit.rediger*`; Codex review in eight rounds, 42 findings, each fixed or made moot by the redesign (stale overwrites, writes nobody made, missed edits, the notice judging a different state from the write, failed saves lost on navigation, changes during the modal notice, re-entrant switches, AI state written on opening or over newer results); edit detection was redesigned once (from guessing the user's intent to saving every change field by field) |
| 9g3a | `a2b3dc7` | — | Agent-verified: the observation form (Rediger, and the dialog still used for new and cloud-only observations) takes the mockup's layout, reusing its widgets: a heading “Observasjonsdetaljer” with a status badge (Utkast / Ferdig, following the draft flag); Taksonomi beside AI-forslag at the top (the existing splitter; Biotop, Substrat, Vokser på stay as tabs, owner 2026-11-12; Uidentifisert, Usikker, Fremmed eller dyrket in one row); a last AI column with Bruk on each suggestion and Valgt on the one that is the observation's full scientific name (Bruk always names the observation, also from Vokser på); date, place, and latitude and longitude on one row beside both comments side by side; the map as one line (▸ Kart opens today's tools in place: the link field, Hent kartlenke, Kart; the place and country on the line; Lim inn kartlenke pastes the clipboard's link and sets the position); today's Sporely Cloud block moved unchanged into “Status og publisering” (9g3b makes it the stepper). The form now fits 1500×950 without scrolling (about 750 px instead of 815). qtbot tests in `tests/test_edit_layout.py` (10); renders `edit.rediger*`, `observations.taxon-picker-*`; Codex review: 4 findings fixed (Bruk set the host from Vokser på, Valgt stale after the picker's changes, two varieties both Valgt, the same map link pasted twice) |
| 9g3b | `9ed6333` | — | Agent-verified: Rediger's Status og publisering (owner 2026-11-12). The steps Utkast → Ferdig → Publisert til tjeneste replace the Utkast checkbox in Rediger (it stays as the form's hidden state; the dialog for new and cloud-only observations keeps it until 9g4): Utkast and Ferdig set the draft flag and save at once through the publish notice; the last step shows the services the observation is published to (from the stored service ids). The hint sits in the panel heading. “Ferdig og publiser til {tjeneste}” with ▾ (Artsobservasjoner, iNaturalist, Begge when both are on; the services enabled in Settings › Nettpublisering; the choice remembered) saves as Ferdig first, publishes nothing if the notice is cancelled or the stored row is still a draft, then runs today's Publish for exactly this observation (`ObservationsTab.publish_observation` pins the publish path's selection checks to it; the list's own selection, export and plate are untouched, and Vis' Publish is off meanwhile). Deling and Posisjon are never changed implicitly. While Publish runs the controls take no second click, observation switches and reloads wait, and only the service ids count as the form's own write. Also fixed: Vis' `_update_publish_controls` used a Both action its own refresh had deleted. qtbot tests in `tests/test_edit_publishing.py` (17; Publish replaced by a recorder, so they check what Rediger asks for, not the uploads); renders `edit.rediger*`; Codex review in five rounds, 12 findings fixed (publishing after a cancelled notice, a second upload from a second click, a sync during the upload masked, a service switched off meanwhile, Both stopping half way when the list changes, Vis publishing the pinned observation, stale Vis controls, a replaced form's queued request, a reload under a running upload, stand-in tabs in existing publish tests, a queued reload while hidden or before a held switch). Human check: publish one real observation from Rediger to each service you use |
| 9g3c | `cef514c` | — | Agent-verified: Rediger shows the strip shared with Vis, Mål and Bilder below the form, above the bottom bar, instead of the form's own photo row (owner 2026-11-12; `EditObservationPage.claim_shared_strip`, channel `edit`; `ObservationDetailsDialog.attach_strip`/`detach_strip`, as Filer's editor in 9f3). The form stays the source of truth and renders its images there; clicking and Ctrl-clicking choose which photos AI looks at (and which image's AI suggestions show), as the own row did, and the chosen image saves with the AI state as before. Images are not changed here: no ×, Delete, reordering, cloud checkboxes or other right-click actions; Edit and a double-click open Bilder › Filer og SD-kort on the image, and Edit images' key also works with a thumbnail focused. Rediger keeps a height of its own for the strip, one compact row (100 px, draggable, remembered), so Status og publisering stays in view at 1500×950; Vis, Mål and Bilder pass their own height around it. The strip follows the page, the observation (each form) and a rebuilt form; an empty Rediger shows no strip. Thumbnails now show whole (the own row clipped them). qtbot tests in `tests/test_edit_strip.py` (10, real clicks and keys); renders `edit.rediger*` (now with three photos); Codex review in two rounds, 3 findings: two fixed (a click in the strip did not let the form keep its AI choice, as a click in its own row did; Edit images' key with a thumbnail focused), one not a regression (the chosen AI image saving a second after the click is what the own row did) |
| 9g4a | `2aa3e05` | — | Agent-verified: every Rediger bilde opens Bilder › Filer og SD-kort for the photo's observation, on that photo (`MainWindow.open_files_for_image`): Vis' and Mål's strip, Live, Kameraimport (its imported photos may belong to any observation; the card's path is resolved to the saved image it was recognised as, by file name and capture time) and Rediger. The observation becomes the one shown, its table row selected before Bilder is shown so the tab change keeps it. A running Live session keeps Bilder on its own observation: another observation's photo is refused with a notice. The modal Kalibrer objektiv and species plate dialogs no longer offer Rediger bilde (Bilder would open behind them; a calibration image was never an observation photo). The Legg til bilder dialog now opens only for Ny observasjon and cloud-only rows (9g4b, 9g4c). qtbot tests in `tests/test_edit_photo_opens_files.py` (7, real right-clicks; the dialog's `exec` fails the test); no layout change, no renders; Codex review: 2 findings fixed (showing Bilder re-activated the table's other observation; Kameraimport passed the card's path, so Filer kept another photo selected) |
| 9g4b | `fe4381d` | — | Agent-verified: Ny observasjon (the button, N, the list's New, files dropped on Vis) opens Bilder › Filer og SD-kort for a not-yet-saved observation (`MainWindow.start_new_observation`, `FilesSourcePage.start_new_observation`); the Prepare Images and New observation dialogs no longer open for it. Nothing is written before “Lagre ny observasjon”: it builds the New observation form off-screen (`ObservationsTab.prepare_new_observation`) so the date, GPS and its attribution, Utkast, Deling and Posisjon are what OK without changes saved (the photo shown gives the date and GPS, as after the dialog's Continue; “Bruk for observasjonen” sets them on it), asks its publish notice and the possible-duplicate question (cancelling writes nothing and keeps the photos), creates the observation through the dialog flow's own body (`create_observation_from_prepared`), then Rediger opens. The place name and country come from Rediger's own lookup on opening (the off-screen form starts none). A new observation can be saved without photos, as before. The observation shown stays active; the header reads Ny observasjon while it is on screen; switching pages keeps it. Choosing another observation (also during a Live session) or Rediger bilde leaves it: its photos are kept until saved or discarded (the status line says so; New shows them again; quitting asks). A failure part way through saving never lets the same photos be saved twice. qtbot tests in `tests/test_new_observation_in_files.py` (16, real clicks; both dialogs' `exec` fail the test; EXIF photos; mutation-checked); renders `images.new*` (nb light/dark, no photos yet, de 1100×720); sync harness scenarios `new_microscope_observation` and `new_observation_without_images` drive the new flow (not run: they need a real account). Codex review in two rounds: 4 findings fixed (a failed save could save the photos twice; Rediger bilde on the shown observation kept Ny observasjon; choosing an observation during a Live session kept it; the harness counted RAW+JPEG pairs as two), 1 rejected (switching to Bilder activates the table's selected row for every way in; the new observation's anchor is that row) |
| 9g4c | `9c8d9e4` | — | Agent-verified: Edit (or double-click, or Rediger bilder) on a cloud-only row shows the cloud observation in Rediger (`EditObservationPage.show_cloud_observation`) with one button, “Importer til denne maskinen”; the Rediger observasjon dialog is retired (its fallback in `edit_observation` is gone; neither dialog opens as a window now). Nothing is downloaded or written before the import (`ObservationsTab.import_cloud_observation`), which does what the dialog's OK did unchanged: the form built off-screen from the cloud values with the cloud row's sharing state as the publish notice's baseline, the cloud row's sync key, the confirmed location precision, the images downloaded and linked to the cloud rows they came from, nothing written to the cloud; then Rediger edits the local copy in place (`open_edit_mode`). A stale inbox row is refused at opening and at the import. The active observation stays active while the cloud row shows (the header names the cloud row there); choosing an observation or an explicit Edit leaves it. qtbot tests in `tests/test_cloud_row_in_rediger.py` (7; the download and cloud client are stand-ins; both dialogs' `exec` fail the test), `tests/test_cloud_inbox_import_identity.py` updated; renders `edit.cloud*` (nb light/dark, de 1100×720; the first render clipped the notice, fixed). Also lists the 9g4b/9g4c review scenarios in the renderer's registry test (missing since `fe4381d`). Codex challenge: 1 finding fixed (Edit on the active observation kept the cloud row), 1 kept as it was before, an open question for the owner: a keyless cloud row (sync_key NULL) is linked only after its images are saved, so an import that fails part way is not recognised and can be imported again (the dialog had the same gap; closing it changes when the link is written, a sync-identity change). Human check: import one real cloud-only observation and confirm it then syncs without a second copy |
| 9h | `c0acf86` | — | Agent-verified: Kameraimport's left column reads Importmappe, Avvik i kameraklokka, Live-lab-økter, Handlinger (the sessions card keeps its stretch). “Sync Shot” is renamed “Camera time offset” (the feature, reusing 9e's translations) and “time code” (the QR, as on the Kun logg page; “Ny Sync Shot” is “Vis tidskode”), “Microscope sessions” and “retrospective microscope sessions” “Live lab sessions”, on the screen and in the time-code dialog (`ui/sync_shot_dialog.py`), in nb, sv and de. The page hint is rewritten and each control has a hover hint in the hint bar (`_register_hints`). The bin button is unchanged (its tooltip uses the new name). qtbot tests in `tests/test_camera_import_layout.py` (3; all fail on the old screen); renders `live-lab.camera-import-*` and `quick-fixes.camera-import-de` (German “Zeitcode anzeigen” clipped its button: shortened to “Zeitcode zeigen”); Codex review: 1 finding fixed (the session list's hint said “in this folder”, but it also lists every observation with recorded sessions) |
| 9i | `7cc07db` | — | Agent-verified: “Fra SD-kort (nye bilder)” beside “Velg filer…” in Bilder › Filer og SD-kort (`FilesSourcePage._from_sd_card`, `utils/card_photos.py`), the owner's three decisions as above. The photos are read with Kameraimport's scanner (RAW and camera JPEG as one photo, the RAW companion preference) and added to the editor unsaved; nothing is saved before Lagre (the remembered folder and the editor's RAW/HEIC staging copies are written, as with Velg filer). A photo counts as imported when any of its files matches; Kameraimport now shares the capture-time key (`capture_key`, unchanged behaviour). Pressing again adds nothing twice and keeps the day; Ny observasjon takes the newest day with new photos. An observation date with an offset (a cloud observation's UTC time) is read as local time. The status line says what was added, or why nothing was (no new photos that day, with the count on other days). The page is busy while the card is read and the photos added. qtbot tests in `tests/test_files_sd_card.py` (10; the card is a folder with EXIF photos and the volume listing a stand-in; mutation-checked); renders `images.files*`, `images.new-small-de`; Codex review in two rounds: 4 findings fixed (a second click while photos were added; pressing twice walked Ny observasjon back a day; a RAW pair imported as its JPEG came back as new; a UTC observation date picked the wrong day). Human check: insert a real camera card, press the button on an observation from that trip, and check that the photos are the right ones (and none already imported) |
| 10a1 | `f59ca98` | — | Agent-verified: dead code removed with no behaviour change (owner 2026-11-12: debug and stale code go): the dead auto-measure path with its “Auto Measure Debug” window; `LanguageSettingsDialog`, `AppearanceDialog` and the standalone openers for Language, Appearance, Database, Profile, plot settings and cloud sync (Preferanser has these pages; the language list's two tests moved to its page); `LoadingDialog`, `SpinnerWidget`, `ScaleBarCalibrationDialog`, `ObservationSelectionDialog`, `RenameObservationDialog`; `_request_settings_calibration_conflict_repair` (its button went in `8df2403`); the modules `ui/stats_table_widget.py` (unparseable: a doubled byte-order mark), `ui/measurement_tool.py`, `ui/observation_list.py`; about 60 other unreferenced methods (found by identifier counts over `ui/`, `utils/`, `database/`, `tools/` and `main.py`, iterated until nothing new appeared; Qt virtual overrides and runtime-built names checked; methods only tests use were kept) and the imports they left unused. About 2,400 lines of code; 95 translation strings went with it. Full suite: 6,942 passed; its two other failures were real bugs from earlier stages, fixed in their own commits: `3e0ed22` (four UI-state settings from stages 6, 9g3b and 9i were missing from the backup policy, so a full backup failed once one was set; `measure_main_splitter_sizes_v2` is set by dragging Mål's divider) and `d02b7e7` (a test of the dialog path removed in 9g4c, replaced by one of Rediger's live path). Codex review: no findings. Human check: run a full backup (Fil › Sikkerhetskopier Sporely…) and check that it completes |
| 10a2 | `588f96b` | — | Agent-verified: the dead reference code removed with no behaviour change: `AddReferenceDialog` (the drawer's three helpers became module functions, same translation context), `CloudReferenceDialog` (`CommunityResultsPane` stays), the `ReferenceLibraryAttachDialog` module, `ReferenceValuesDialog`, the `MainWindow` handlers that opened them or drove the old reference panel, helpers left with no caller (iterated until none appeared; tested data-layer methods kept), and 28 review scenarios built on them. About 6,700 lines of code and tools and 5,900 of tests. Coverage was compared before and after with a stdlib line tracer (`sys.monitoring`): the tests that reached live persistence through the removed panel's Add/Edit now drive the live paths (`_submit_reference_editor_result` as the create dialog's “Add to plot” calls it, `_edit_reference_series_row`; a removed guard makes the drift test fail), and new direct tests cover what lost its only tests (the community pane's exclusion of the observation's own dataset, `_attach_normalized_reference_outcome`'s rollback-failure and already-attached branches, the comparison list's visibility toggle, the enhanced-statistics end-to-end test without its picker step). Full suite: 6,731 passed (the two renderer tests need a worktree `.venv`). Codex review in two rounds: 6 findings fixed (fixtures and the scenario fixture patched removed module globals; two tests still needed removed handlers; two coverage gaps above; stale `__all__` entries in two modules), none open. Not fixed (noted): `nb_NO` “Same taxon” in `observations_tab.py` is untranslated (pre-existing) |
| 10a3 | `7444645` | — | Agent-verified: dead code removed with no behaviour change. `CloudSyncDialog` (the module; no opener since 10a1): its behaviour lives in Preferanser's Sporely Cloud card and the observations tab's sync completion, which already had most of its tests; new ones cover the rest (the signed-out card offers browser sign-in only; Log in starts the OAuth worker and closing the waiting dialog cancels it; the original-upload summary shows only when enabled). `ObservationDetailsDialog`, now only Rediger's page or the off-screen New observation form: the window Save's unfinished-search question, the edit-images handlers' window branches (they always ask the page now), the form-level genus/species/vernacular and host handlers that `TaxonInputController` replaced (never connected; they looked alive only through the old reference panel's same-named methods, gone in 10a2), image-list accessors with no caller, and 19 helpers left without one (iterated until none appeared); also `TaxonPicker.keep_unfinished_as_name` and a dead controller helper. About 1,800 lines of code; 14 translation strings. Tests that called removed handlers drive the real signals (field text, the completers' `activated`, `editingFinished`) or became controller tests; a line-trace comparison found three live branches that lost their only tests, now covered (Unidentified drops typed search text; removing the species clears the stale common name; a common name without a species leaves the taxon fields), and every edit-images entry is tested in place. Full suite: 6,713 passed (the two renderer tests need a worktree `.venv`). Codex review: no bugs; its remark that Up does not wrap to the picker's last row was fixed in the tests' keep-as-name helpers. Left for 10a5 (found here): the form's remaining window mode and the hidden fields' completer plumbing. Not changed (noted, pre-existing, from reading the code): in Preferanser, cancelling browser sign-in shows “Login Failed” and records a sync error; the old dialog returned quietly to signed out |
| 10a4 | `d1d9310` | — | Agent-verified: dead code removed; one invisible change. The old reference panel's widgets, completers, models and taxon lookup were never created after stage 7, so every branch guarded on them and every method that read them was dead: they go from `MainWindow` with the panel's AI-suggestion combo path and the helpers left without callers (iterated until none appeared; tested data-layer and widget methods kept). `load_reference_values` now loads the observation's own taxon, as it always did in practice (its keep-the-panel's-taxon branch could not trigger); the always-empty `"reference_panel"` key is no longer written to the local Analysis UI state (`gallery_settings_<id>`), which only the removed branch read. The invisible change: the species-availability cache had no reader left, so its refreshes go (two whole-table queries on every switch to Analysis, after reference saves and after a Rediger taxon change), with Rediger's taxonomy-changed flag that existed only for it, and the restore path's close loop for the never-created lookup. The hidden preview stack: `ReferencePreviewPane` and `ReferenceComparisonView` (modules deleted), the editor's and the community list's preview code, and 4 review scenarios. About 3,200 lines of code and tools and 1,200 of tests (net); 118 translation strings. Tests of removed code went; tests that used the panel's fields as setup run without them; preview assertions about what the editor read from a paste now check its `MeasurementContent`; a line-trace comparison found the community list's summary-or-raw-points payload had lost its only test (new test), and typing readiness is tested without a preview. Mutation checks on three retargeted tests fail as they should. Full suite: 6,687 passed (the two renderer tests need a worktree `.venv`; the registry's 144 IDs were checked against `--list` by hand). Codex review: no findings. Renders of the drawer's Community page and the create dialog are unchanged. Kept (noted): `references/reference_comparison.py`, the comparison model the preview drew, has its own contract tests but only `point_extents` is used by the app; `SpeciesDataAvailability` keeps its emoji constants and tested methods |
| 10a5 | `db8c1dc` | — | Agent-verified: dead code removed; one small behaviour change. `ObservationDetailsDialog` is only Rediger's page or the off-screen New observation form (which calls `confirm_publish_change` itself), so its window mode goes: Save and Cancel, `accept`'s publish-notice close, `reject`'s window branch (Esc always keeps the page), `done`'s cleanup (only reached through those; `closeEvent` and the explicit cleanup remain), the window title, size and saved geometry (the gallery and taxonomy splitters still restore and save), and the Save hint. Behaviour change: Return/Enter shortcuts that pressed the hidden, disabled Save also swallowed Return in the form's plain fields; Return now finishes a field as Tab does (the Grows on name fields, the coordinates); the picker and notes were never affected. The hidden genus, species and common-name fields (behind the taxon picker; never focused) lose their placeholders, label, completer aliases and popup styling, the identity-offer hooks on their editing, and the genus autofill from a typed common name (`_on_vernacular_editing_finished_populate_genus`) with its tests; `TaxonInputController` is unchanged (the visible Grows on fields use it, completers included). `TaxonPicker.set_ai_suggestion_handler` and `AiSuggestion.key` (no host set one) go; a chip without a handler was already tested. About 250 lines of code; 10 translation strings. The publish-notice tests drive `confirm_publish_change`; an Enter test opens Grows on, types in the focused field and checks Return finishes it once and keeps the page (Codex's review: the first version sent Return to a hidden field). Mutations caught: a Return shortcut put back fails the Enter test; ignoring the notice's answer fails four publish-notice tests. Renders: Rediger (nb, small de, cloud row) pixel-identical; the two `observations.*` scenarios render the bare form as a window and lose the Cancel/Create buttons the app never showed. Full suite: 6,680 passed (its one failure, an identity-offer test that typed into the hidden Scientific field, now sets the field as the app does: Link re-checks the name on click, and disabling that re-check fails it and its neighbour; the two renderer tests need a worktree `.venv`). Codex: two test defects (the obsolete autofill tests; the hidden-field Enter test), both fixed. Left for 10a6 (found here): the typed-name Red List match behind `_on_taxon_manual_editing_finished`, also hooked to the hidden fields. Noted, not changed: no host feeds the picker's Artsorakel chip strip (`set_ai_suggestions` has no app caller); the form's AI table offers the suggestions |
| 10a6 | `958c0da` | — | Agent-verified: dead code removed with no behaviour change. The typed-name Red List match: `_on_taxon_manual_editing_finished` listened to the hidden genus and species fields' `editingFinished`, which has not fired since the taxon picker hid them, so `TaxonInputController`'s display-only name match (set, read, clear and five resets) was never set and the form's two readers always got nothing (the Red List staleness token's extra element; the resolve's fallback when no proven identity is committed). All go; a name still binds identity only through an explicit pick, and the badge only follows a proven identity, as since the picker. `TaxonLookupService.resolve_manual_scientific` stays (tested data layer, now without an app caller), as do the controller's own editing-finished handlers (the visible Grows on fields use them). About 170 lines of code; no translation strings. Tests: the mirror of the hook in the Stage 3B.3 invariant tests and the display-only tests go; the typed-name badge test becomes the picker's “keep as name” path (binds nothing, the name kept whole, and a badge already shown or still pending goes; dropping the host's clear fails it); the boundary test asserts the channel is gone. A line-trace comparison found that the removed typed-name test was the only end-to-end test of the deferred Red List resolve showing a badge: a new test picks a proven concept through the picker's signal and gets its Norwegian status (VU); skipping the badge write or disconnecting the signal fails it. Full suite: 6,676 passed (the two renderer tests need a worktree `.venv`). Codex challenge review: no production defect; its two test findings (keep-as-name started without a badge; the pick test bypassed the picker's signal) are fixed, and test comments that still described the name resolver are updated |
| 10b | `f20285a` | Owner's layout opinion pending | Agent-verified behaviour; the layout waits for the owner. The shared notice lives in each screen's bottom (hint) bar, as decided: `HintStatusController.show_notice(text, tone, details=, undo=, undo_text=, timeout_ms=)` and `dismiss_notice()`. It shows at once, also over the hint of the control just clicked; Details opens the text below the message, inside the bar; Undo dismisses and runs the callback (a failing one shows an error notice with the exception); ✕ dismisses. Info and success clear after 6 s (10 s with Undo; opening Details keeps it); errors and warnings stay until dismissed or replaced; a newer notice replaces an older one. Hover hints still work: one covers the notice while the pointer is on a control, and the notice comes back, Details still open, when it leaves; a timed status shows over it the same way. `HintBar` gains the controls (hidden otherwise: a bar without a notice is pixel-identical on Rediger, Analyse, Live Lab, Vis, Mål, Bilder and Preferanser, and keeps its size hint, which Live Lab sizes its stack from); `HintProgressStack` (the Analysis gallery) makes room while a notice shows and gives it back; hover hints never grow it. No caller yet: 10c–10f move message boxes onto it. Five translation strings (Details, Undo, Dismiss, Could not undo.). Tests (`tests/test_hint_notice.py`, 14, real widgets): every rule above, plus the three Codex findings; five mutations of the rules and three of the fixes each fail a test. Review scenarios: `notices` group (7: error, Details open, Undo, dark, small window in German, the Analysis gallery). Full suite: 6,690 passed (the two renderer tests need a worktree `.venv`; the registry test, in process, lists the 151 scenarios). Codex review: three bugs, all fixed with tests (the gallery slot missed a notice as tall as the hint before it; refreshing the bar closed an open Details; a notice timing out after its page was deleted wrote to the deleted bar) |
| 10c | `00ada52` | Owner's layout opinion pending | Agent-verified behaviour; the layout waits for the owner. Kalibrer objektiv has no information or warning box left (30 removed: 28 calls and two wrapped info dialogs); its confirmations stay dialogs (unsaved changes, deletes, the in-use dialogs, recalculating measurements). New objective: what is missing (name, magnification, NA, sensor width, image width, a duplicate name) is said in red under the form and the cursor goes to the field; editing clears it. Calibrate, Export image and Add measurement are disabled until there is an image (also while a cloud reference preview shows), and their bottom-bar hint says why; Set as Active waits for a calibration chosen in the history. During auto calibration or an export, Close is disabled; Esc or the window's close button wait as before and say why under the progress (the bar is hidden then). Failures (images that would not load, export, a missing image file, files a delete left, invalid results) are notices with Details; “auto calibration available” and “measurements updated” are info and success notices. Shared notice change: a notice's countdown waits while its bar is hidden (a host's progress panel), so it is never cleared unseen. One new string (“Could not export the image.”), 20 box titles dropped. Tests (`tests/test_calibration_notices.py`, 18, real dialogs; the run tests drive the real start paths): every rule above; nine mutations each fail a test. Review scenarios: `calibration` group (5: no image, light and dark; export failure with Details, nb and de; New objective's message). Seen in them, not changed: with Details open in the dialog's minimum window the Results card loses its last row until Details closes; Crop and Clear stay enabled without an image (as before). Full suite: 6,709 passed (the two renderer tests need a worktree `.venv`; the registry test, in process, lists the 156 scenarios). Codex review: three bugs, fixed with tests (the run start paths disable the controls before setting the running flag, so the new state check re-enabled them; a cloud reference preview left Calibrate and Export enabled; an info notice posted during a run could time out behind the progress panel) |
| 10d1 | `67487ed` | Owner's layout opinion pending | Agent-verified behaviour; the layout waits for the owner. Preferanser's own pages (the login and publishing half is 10d2). The Profile page has a bottom bar of its own, shown like the other pages' (first tried: the publishing dialog's bar, whose Sporely Cloud card the page holds; the render showed that bar brings the publishing services' “Ikke innlogget” onto the Profile page). The profile photo (sign in first; updated; upload failed, with the error in Details), the cloud-sync buttons (not available; already running) and a profile that did not reach Sporely Cloud on Save are notices there; Save from another page switches to the Profile page to show it, and Preferanser stays open as before. The sign-in and session handling is unchanged (only the message: `_profile_sync_error_notice` returns the message and, when it adds anything, the error for Details). Kept a message box: “Database Move Failed” (Preferanser closes on Save, and it is about moving the database files). One builder, `build_sync_log_dialog`, makes the Sync log for the main window and Preferanser (Preferanser's fallback gets the main window's size and OK button). 7 boxes removed; two new strings (the message parts without the error), 7 dropped. Tests (`tests/test_preferences_notices.py`, 7, the real hub; three profile-save and Sync log tests retargeted): five mutations each fail a test. Review scenarios: `preferences` group (4: the Profile page, and not synced with Details open in light, dark and German). Full suite: 6,714 passed; its two other failures were the username-conflict cases Codex flagged, fixed after the run started (9 of 9 in that file now; the two renderer tests need a worktree `.venv`). Codex review: one finding, a test still calling the renamed helper, retargeted to check the Details too |
| 10d1 follow-ups | `487df60`, `1c8fe72` | Done | Owner's reports after 10d1. (1) A sync started from Preferanser while Rediger showed had no progress on screen: Rediger is its own tab since 9g2 and the progress lived only in Observations' bottom bar. Rediger now borrows the panel (bar, percentage, Cancel, text) above its bottom bar while it shows; Observations takes it back (`ObservationsTab.lend_status_progress`/`reclaim_status_progress`, `EditObservationPage.claim_sync_progress`, `MainWindow._place_sync_progress`). Tests `tests/test_sync_progress_in_rediger.py` (4, real `MainWindow`, stand-in worker); disabling the tab hook fails two. Review scenarios `edit.sync-progress` (light, dark, small window in German). Still without sync progress, as before 9g2: Mål, Analyse and Bilder. Codex: no findings (suggested the sync-ending-in-Rediger test, added). (2) Kalibrer objektiv's “Sampling: …” stayed English: now Samplingsgrad (nb, sv) and Abtastung (de); Swedish's sampling states were Norwegian words (now Undersamplad, Översamplad, Kraftigt översamplad); two Norwegian typos fixed |
| 10d2 | `a918a22` | Owner's layout opinion pending | Agent-verified behaviour; the layout waits for the owner. `ArtsobservasjonerSettingsDialog` has no information or warning box left (22 removed); the two confirmations stay dialogs (reset the account link, remove saved passwords). Sporely Cloud's sign-in, sign-out and account-link reset messages show in the Profile page's bar, where the card is (`_show_cloud_notice`: Preferanser is an ancestor of the embedded dialog, not its parent; standalone, the dialog's own bar); the publishing services' login and sign-out failures (“Could not sign out of {service}.”) and the saved-passwords result show in Online publisering's bar (`_show_notice`), each error under Details. A sign-in into another account's database says so in the message, the full reason in Details. A result arriving after its window closed (a sign-in finishing late) shows as a box, as before, so it is not lost. The credential prompts (Artportalen, Artsobservasjoner, Mushroom Observer's keys) say what is missing in red under the form and put the cursor in the field; typing clears it. The Artportalen and Artsobservasjoner prompts are now translated (context `CredentialPrompt`, both modules added to `tools/update_translations.sh`), with the app's own OK and Cancel. Sign-in, session, credential storage and account linking unchanged (Codex compared old and new prompts line by line). Removed: the uncalled Artportalen cookie-import dialog and its parser (2 of the 6 prompt boxes), `_on_login_success`'s cookie-saving branch (every caller had already saved). 26 new strings. Tests (`tests/test_publishing_login_notices.py`, 16, the real hub and the real prompts driven while modal; 4 retargeted): nine mutations each fail a test. Review scenarios: `preferences` group, 5 new (a failed service login with Details, light and dark; a failed cloud sign-in on the Profile page; the prompt's message in nb and de). Full suite: 6,732 passed (the two renderer tests need a worktree `.venv`); after the Codex fix, the 233 tests that build these dialogs pass. Codex challenge review: a notice could be lost when the sign-in finished after Preferanser closed (fixed, with tests); the saved-password path was untested (tests added); error text can carry a server's response into Details, as the boxes did (not changed). Found, not changed (owner question): Preferanser's `_on_cloud_login_changed`/`_on_cloud_logout_changed` hooks are reached through `self.parent()`, which is a page widget, so they never run in the app: after signing in or out in Preferanser the main window's cached Sporely Cloud client and avatar are not updated |

Found during stage 9g3c, not fixed there (rule 1):

- Rediger's strip shows the observation's saved images only: Live's and
  Filer's unsaved images for it show in Vis, Mål and Bilder, not here (AI
  looks at saved images).
- In a small window (1100×720) the strip's 100 px make the form scroll
  further; the strip cannot be made lower than 100 px (the shared strip's
  minimum).

Found during stage 9g2, not fixed there (rule 1):

- Rediger keeps today's form layout, which needs about 815 px of height: a
  small window scrolls the form (the bottom bar stays in view since the
  follow-up below). 9g3 brings the mockup's layout.
- The form's own image row stays (it chooses which photos AI looks at; no
  removing or reordering here); the shared strip comes with 9g3's layout
  (done in 9g3c).
- The list, table and title refresh when Rediger is left or the
  observation changes, not on every save.
- After a save the dialog's flow also uploaded pending Artsobservasjoner web
  images; Rediger changes no images, so it does not.
- `_persist_ai_state_now` now skips writing an unchanged AI state, in the
  dialog too (opening wrote it again before).

The owner's first try of 9g2 (2026-11-11), fixed in a follow-up commit on the
same branch (agent-verified; tests in `tests/test_edit_mode.py`, renders
`edit.rediger*`, timings on a copy of the owner's database with cloud sign-in
blocked):

- Slow to open (about 5 s reported; 1.3–2 s measured here, offscreen and
  cocoa). Rediger rebuilt the form every time it was shown again; it now
  keeps it unless the observation's row, its images or the name language
  changed meanwhile (coming back: 0.04 s). The form's own writes (its saves,
  AI results it stores) are folded in column by column, so a change made
  elsewhere before a save still reloads it. The AI table looked each suggestion's common name
  up again on every redraw; it is looked up once per form. Most of the
  remaining cost is the form's own setup, chiefly about 225 short-lived
  database connections and a slow local taxon-record query in
  `database/taxon_lookup.py`, which other screens share; not changed here.
- The bottom bar was out of view and the page scrolled into an empty band:
  the dialog's outer layout kept stretches from an older three-row layout,
  giving the bottom row more spare height than the form, and in place the
  save status let that row grow. The stale stretches are gone and Rediger
  pins the bottom bar below the scrolling form (Edit images' shortcut works
  from it too).
- “Changed 1045 to private, then public; nothing happened”: the profile has
  the publish notice switched off (“Don't show this again”; Settings ›
  Online publishing restores it). Rediger behaved as designed.
- “In Bilder I no longer see image details (such as folder location)”: not
  yet identified; asked the owner which panel they mean.

Still 4–5 s for the owner after that: on a signed-in profile every form's
deferred setup fetched the cloud usage summary (the plan profile and an exact
count of private observations, two Supabase requests) on the GUI thread, for
the informational private-slot line only. Forms now share the answer, kept on
the window's cloud client for three minutes (a new sign-in makes a new
client). The first opening after that still waits for the two requests;
moving them off the GUI thread touches the client's token refresh and is left
for a decision. Codex: no findings.

Codex reviewed the follow-up in two rounds, five findings, all fixed: a
save's snapshot of the whole stored row hid changes made elsewhere (a later
point move could have written a stale longitude back); a changed name
language and AI results the form stored itself were not considered; the AI
common-name cache was keyed by the setting rather than the lookup's language;
Edit images' shortcut did not reach the moved bottom bar. Related tests: 531
passed (the two renderer tests that start `.venv/bin/python` in the worktree
need the project venv there; CI runs them).

Found during stage 9f3, not fixed there (rule 1):

- New images keep the order they were added in; the strip does not drag
  unsaved images (9c's rule), where the editor's own gallery did.
- Filer's strip shows what its editor edits: Live's unsaved captures for the
  same observation show in Live, Vis and Mål, not in Filer. Filer's new
  images show everywhere.
- Dropping files on the strip in Filer does nothing (the new-observation drop
  belongs to Vis); “Velg filer…” adds them.
- The cloud checkboxes are gone from Live's strip too (a click there was never
  saved). Wiring them in Live and Filer, if wanted, is a sync-adjacent change
  for its own stage.

Found during stage 9f2, not fixed there (rule 1):

- In Filer the observation's own time and GPS fields are hidden; they move to
  Rediger in 9g. Until then the Rediger dialog edits them.
- An observation without GPS takes the first new photo's GPS when saved
  (the editor did this before 9f2 too); one with GPS keeps it unless
  “Bruk for observasjonen” is pressed.
- Preparat stays a side card (for microscope images); the mockup's status
  line puts it with the microscope bar. The editor's own gallery still labels
  thumbnails “Field” in English; it goes in 9f3.

Found during stage 9f1, not fixed there (rule 1):

- Some Legg til bilder tools write at once, not on save (AI crop to the
  database, scale-bar calibration, rotating a saved JPEG), so “Forkast
  endringer” cannot undo them; the dialog's “Forkast” had the same limit.
- Quitting right after starting an AI suggestion: its worker can run up to
  30 s while the app waits 5 s at exit (also when the dialog was a window).
- The main window's minimum height is set by Kameraimport's page (703 px),
  also before stage 9.

Found during stage 9d, not fixed there (rule 1):

- A Live session's teardown with unsaved RAW captures never goes idle in
  tests (`drain_live_lab` times out; also before 9d); the new tests save
  their captures first.
- Deleting an observation that has unsaved captures in a running session:
  saving them later hits the foreign-key constraint (Codex, pre-existing).

Found during stage 9a, not fixed there (rule 1):

- Kameraimport: excluding a photo from the staging gallery (Delete) adds it
  to the excluded set but leaves it in the gallery until the matches are
  recomputed (the waiting count already leaves it out).

Found during stage 7, not fixed there (rule 1):

- Selecting another observation's image through a Measure badge makes it
  active without reloading `reference_series`, so the Analysis plot and
  comparison list can still show the previous observation's references.
  The reference drawer reads attachments from the database and is not
  affected; the plot is.
- Analysis plot: the legend is clipped at the plot's left edge, and the
  first comparison row's icon overlaps its date line (visible in the
  `analysis.tab` render).
- After 7d nothing in the app opens the old picker: `AddReferenceDialog`
  (`ui/add_reference_dialog.py`) and `MainWindow._on_add_reference_clicked`
  remain only for their tests and `reference.add-dialog-*` scenarios, and
  the drawer still borrows three helpers from that module. Remove them
  together, moving the helpers, in a cleanup stage.
- Stage 8 follow-ups (pre-existing, found in its reviews):
  - Clearing an image's calibration does not reach the cloud: the image
    push omits `calibration_uuid` when the local link is empty, so the old
    calibration stays attached remotely (Mål's objective change and the
    batch both clear it when the new objective has no active calibration).
    A sync contract change.
  - Loading an image whose scale is within 1 % of its objective's writes the
    objective's scale to the database without rescaling (`apply_image_scale`
    → `apply_objective` with prompts suppressed).
  - The calibration-measurement edit rescaler (`_on_calibration_measurement_edited`)
    replaces `notes` with “Q=…”, which drops a multi-line measurement's
    polyline. It, the import dialog and Observations import still have their
    own rescale code; moving them to `apply_scale_changes` is the next step.
- `MainWindow._submit_reference_editor_result` still adds a legacy plot
  series and reports success after the shared persistence helper rejects a
  taxon drift for individual spore points. The two-step dialog checks the
  taxon before calling it; the old Quick add / Edit dialog does not.

Found during stage 6, not fixed there (rule 1):

- A full default render (`python -m tools.render_review_screenshots <dir>`)
  does not finish locally within 400 s, also on `021b303` before stage 6; it
  stalls after the raw-processing/portable-import scenarios. CI skips
  `tests/test_render_review_screenshots.py`, and group or scenario renders
  work. Worth a look before the renderer is relied on for whole-app runs.

Found during stage 0, not fixed there (rule 1):

- `tools/update_translations.ps1` lists 14 of the 37 modules the bash script
  scans and runs lupdate with `-no-obsolete`, so running it on Windows would
  delete translations.
- The app loads no Qt base translations (`main.py` installs only
  `Sporely_<lang>.qm`), so standard dialog and message-box buttons (OK,
  Cancel, Yes, No, Close) stay English in every language; 15
  `QDialogButtonBox` standard-button uses in `ui/`.
- 168 `nb_NO` entries are identical to their English source. Many are
  legitimate (product names, Latin terms, format strings); real gaps are
  mostly Analysis plot settings, Parmasto texts and the duplicate-observation
  prompt. Some `sv_SE` entries reuse Norwegian words (“Felt”, “Forkast”).
- Analysis panel at narrow widths: other controls clip as well (“Histogram”,
  “Kernel density”, the export toolbar), and the gallery “Sort” combo renders
  empty in the owner's screenshot.
- Tests and the renderer that build `ImageImportDialog` write the user's real
  `QSettings` (`splitter/ImageImportDialogBottom`) unless `QSettings` is
  patched in `ui.image_import_dialog`; `tests/test_sample_source_ui_presence.py`
  still does. Tests get an isolated app-data folder from `tests/conftest.py`
  (`SPORELY_APP_DATA_DIR`), so `objectives.json`, databases and thumbnails
  are safe there; the review renderer sets no such folder, so a scenario
  group that builds widgets reading them must isolate them itself (the
  `quick-fixes` and `live-lab` groups use `tools/review_ui/live_lab_harness.py`).

Found during P1, not fixed there (rule 1); the first two are pinned as
`xfail(strict=True)` tests in `tests/test_live_lab_behaviour.py`:

- Fixed in stage 2: Live Lab forgot the last objective on restart unless it sorted first:
  `_populate_objective_combo` fills the list while
  `currentIndexChanged → _save_objective_selection` is connected, so the
  first objective is saved over the stored choice before it is read back.
  Stage 2 (capture-state bar) touches this code; fix it there or before.
- Live Lab captures store no `session_id` in `lab_metadata`:
  `start_session` snapshots `_session_microscope_state` before it assigns
  `_session_id`, and later captures inherit the snapshot. Session logs and
  Kameraimport matches do carry it; only the portable archive reads it from
  images today.
- Kameraimport in the dark theme at 1440×900: the side panel gets a vertical
  scrollbar that covers the right edge of its fields; the light theme has
  none at the same size (baseline renders `live-lab.camera-import-*`).
- Offline session start shows the viewer's “Last inn et bilde for å begynne”
  placeholder rather than the “Offline session” title it sets.
- The review renderer's `reference-library` group takes about 180 s (also on
  `main`), so `test_default_renderer_emits_central_manifest_and_all_established_states`
  exceeds its 120 s timeout; CI skips that file.

Found during P2, not fixed there (rule 1):

- `tests/test_live_lab_raw_controls.py` (`_build_raw_controls_state`) assigns
  fakes for `save_raw_preview_jpeg` and `render_raw_preview_proxy_rgb` directly
  on the Live Lab modules instead of through `monkeypatch`, so they stay in
  place for every later test in the same run. P2 kept the behaviour and only
  points the assignments at the module that now uses them.
- The companion grouping (`_queue_companion_source`, `_flush_companion_group`,
  `_handle_raw_companion_source`) and the RAW+JPEG source preference stay in
  `LiveLabTab`: they decide which file of a capture is ingested and serve JPEG
  captures too. Stage 5 (slimmer RAW panel) may want them next to the RAW
  mixin.

Found during P3, not fixed there (rule 1):

- `apply_palette` in `ui/styles.py` still sets its `QPalette` colours from
  literals, many of them equal to colour tokens; it should read
  `_COLOR_TOKENS` too.
- In the Live Lab viewer the “Bruk til kalibrering” bar covers the objective
  tag drawn in the viewer's top-right corner (visible since the white 100x
  tag got its outline). Stage 2 replaces that tag with the live-view label.
- Several renderer scenarios are not deterministic: clock times
  (`observations.taxon-*`), random UUIDs and the order of equally sorted rows
  (`reference.library-manager`, `quick-fixes.reference-library-long-labels`),
  and a line-edit clear button (`reference.add-dialog-manual-range-dark`).
  Before/after comparisons have to ignore them until the fixtures pin them.
- Compact gallery badges (84px thumbnails) clip a tag such as “100x BF”:
  its text is wider than the badge's width cap (49 vs 44 px on macOS), with
  or without the new outline.
- Inline colours elsewhere still repeat token values (for example the
  `#e67e22` measure status text in `ui/main_window.py`); they move to tokens
  as each stage touches their screen.

Found during stage 1a, not fixed there (rule 1):

- Live Lab's image-note field keeps the last selected image's note after the
  viewer is cleared or a new session starts (`_clear_session_viewer` does not
  reset it); typing into it then edits nothing. The Notes summary only counts
  the field while an image is selected.
- The Notes summary's clock time comes from the real session log, so the
  `live-lab.sections-collapsed*` renders differ in that time from run to run.
- On macOS, Tab reaches the section headings (and other buttons) only with
  Full Keyboard Access on; clicking and Space/Enter on a focused heading work
  everywhere.

Found during stage 1b, not fixed there (rule 1):

- German translates “Stain:” as “Faerbung:” in other screens (fixed to
  “Färbung:” only in Live Lab's Preparation section).
- Pill-mode selectors (objective, contrast and the other non-chip ones) use
  Qt's exclusive-group arrow-key handling, which moves focus and clicks the
  next pill; in Live Lab the RAW review shortcuts take Left/Right first while
  a RAW capture is under review.
- No cocoa real-input run was made for the chip and heading keyboard
  handling; it is tested offscreen with Live Lab's real shortcuts installed.

Found during stage 2, not fixed there (rule 1):

- The live-view label follows the sidebar controls, as the old viewer tag
  did: after a capture, changing the objective relabels the image on screen
  with the next capture's objective, and a mixed multi-selection hides the
  label. Whether the label should describe the displayed image or the
  microscope is a design question for stage 9's staging strip.
- Pinch (native gesture) events over the floating bars reach the viewer with
  the bar's own coordinates; wheel events are re-anchored since stage 2.
- On macOS a click does not give a button keyboard focus (platform
  convention), so chips and bar buttons are reached with Tab only under Full
  Keyboard Access.
- The cocoa real-input harnesses post keys with `CGEventPostToPid`, which
  needs the running app to be trusted for Accessibility; this session is
  not, so `taxon_picker_mac_check` cannot type here. `live_lab_mac_check`
  posts its keys as NSEvents in its own queue instead, which needs no
  permission.

Found during stage 3, not fixed there (rule 1):

- A pending RAW preview shows no scale bar: the preview is downscaled and the
  capture does not know its full image size, which the calibration refers
  to. It gets one once saved. Reading the RAW header's size would allow it.
- The “Kalibrer…” link opens the main window's calibration dialog; Live Lab
  then reloads the objectives. The dialog does not report a cancelled run,
  so the reload also happens after Cancel (harmless: the selection is kept).

Found during stage 4, not fixed there (rule 1):

- Contrast memory applies only while choosing the next capture's state:
  editing a selected image's objective in the side panel never changes its
  contrast or the memory. Whether the memory should also suggest a contrast
  for such edits is left for the owner's hands-on test.
- The Notes log shows changes to objective, contrast, mount and stain (the
  plan's list); condition and source changes are logged in `session_logs`
  but not shown.

Found during stage 5, not fixed there (rule 1):

- In RAW review mode the RAW panel edits the selected pending capture
  whenever one exists, even with the viewer showing something else; only
  without pending captures does it edit the next capture's settings.
- “Egendefinert HB” is clipped by the “Velg” button in the narrow side panel
  (also before the redesign).
- A selected-preview request that is already running is not cancelled when
  its settings change; its result is discarded and a new render follows.
- Running the RAW-control tests and the Live Lab tests in one process can
  crash at exit when stale bytecode from quickly rewritten modules is left in
  `__pycache__`; clearing it fixes the run (seen while bisecting stage 5).

Baseline renders for the pilot's before/after comparisons (P1, 2026-11-08):
`/Users/sigmundas/Documents/Code/sporely/sporely-py/docs/plans/november-ui/baseline-p1/`
(gitignored, local only).
