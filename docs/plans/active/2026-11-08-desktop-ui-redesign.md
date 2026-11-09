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
- **Drawers:** left = navigation, right = tools. They overlay the content
  without resizing the layout, one at a time, and close with Esc, the close
  button, or by finishing the task.
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
| Legg til referanse (`AddReferenceDialog`) | Picking: right drawer over the Analyse plot. Creating: two-step dialog | 7 |
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

- **One stage at a time,** each on its own branch and PR. The app builds, runs
  and is usable after every stage. No big-bang rewrite.
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
7. Codex review at each stopping point; visible UI changes wait for the
   owner's layout opinion before merge.

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
| 9 | Observation workspace (the structural change) | Layout opinion; the Kameraimport bin change needs owner approval first |
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
  per contrast method (automatic with a lock, or suggested only: open decision
  below). Hold a key (mockup: B) to see the image before processing. RAW panel
  docked by default, pin toggles overlay, choice remembered.
- Done when: changing contrast switches (or suggests) the preset; manual
  tweaks stay until the next contrast change; the before/after key works.

## Rollout

### 6 — Mål

- Scope: measuring tools (Linje, Rektangel, Flerlinje, start/stop) become a
  toolbar above the image; Skala becomes a summary line; Visning and Info move
  to a right drawer; the left column is removed.
- Done when: the image area is wider; every previous Visning option is still
  reachable; Esc closes the drawer.

### 7 — Reference picker and create dialog

- Scope: picking becomes a right drawer over the Analyse plot, with search, a
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

### 9 — Observation workspace (the structural change)

- Scope: modes in the Observasjoner tab (Vis, Rediger, Bilder, Mål, Analyse)
  with one shared image strip; the list collapses to a rail in Bilder, Mål and
  Analyse with a left drawer; Tabell as a sortable overview sharing data and
  selection with the list; ⌥O, ⌥L, ⌥M, ⌥A map to modes; Live Lab becomes the
  Live source of Bilder (Kamera tilkoblet and Kun logg session types), Legg
  til bilder becomes the Filer og SD-kort source, Mål and Analyse become modes,
  and the top tabs go; Kameraimport stays its own screen, a distinct button at
  the far right of the mode bar (⌥C), with the layout order Import folder,
  camera time offset, sessions, actions. Retire the Rediger observasjon and
  Legg til bilder dialogs. Rediger keeps today's save behaviour: autosave vs.
  ask is undecided, and neither is built until it is decided.
- Split into sub-stages that each leave the app usable (likely: mode shell and
  shared strip; Bilder sources; Rediger mode; Kameraimport); propose the split
  before starting. The monolith panels this touches in `ui/main_window.py` and
  `ui/observations_tab.py` are extracted first, each as its own mechanical PR.
- Settle first: unsaved images when switching observation; the owner's
  approval of the Kameraimport bin change (below). Status og publisering
  changes only `is_draft`; a change that would touch `sharing_scope` or the
  sync contract stops and asks.
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
- **Kameraimport bin button:** recommended design is to hide the row by
  default and offer a separate delete that confirms with the number of
  sessions and events that will be lost. Not built until the owner approves
  (deletion change).
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
- **Rediger save model:** autosave or ask stays open; build neither.

## Open decisions (settle before the stage named)

- [ ] Shortcut keys for objectives and contrast methods (before stage 2).
- [ ] RAW preset on contrast change: automatic with a lock, or suggested only
      (before stage 5).
- [ ] Unsaved edits in Rediger: autosave, or ask when switching (before 9;
      neither is built until decided).
- [ ] Switching observation with unsaved images in Bilder: ask, or save them to
      the observation they were taken for (before 9).
- [ ] Approve the Kameraimport bin change (before 9).
- [ ] “Sync Shot” or “Camera time offset” as the name (before 9; stage 0
      leaves the name alone).
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
  main-image surfaces.
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
  Actions; update button hints (Sync Shot name: open decision).
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

Record each stage as it lands: PR, merge commit, verification tier, manual
test status.

| Stage | PR | Merge | Status |
| --- | --- | --- | --- |
| 0 | sigmundas/sporely#65 | `a682f6c` | Agent-verified (tests, inspected renderer screenshots); owner tested it 2026-11-08 and said to go on; merged 2026-11-09 |
| P1 | sigmundas/sporely#67 | `3162501` | Agent-verified (tests, inspected renderer screenshots); no production code; merged 2026-11-09 |
| P2 | — | — | Agent-verified: Live Lab, RAW and main-window tests green; `live-lab` renders pixel-identical to the P1 baseline in both themes; translations moved to the new contexts with none lost |

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

- Live Lab forgets the last objective on restart unless it sorts first:
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

Baseline renders for the pilot's before/after comparisons (P1, 2026-11-08):
`/Users/sigmundas/Documents/Code/sporely/sporely-py/docs/plans/november-ui/baseline-p1/`
(gitignored, local only).
