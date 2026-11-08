# Desktop UI redesign

Status: **draft; direction decided, concept stage D0 next.** The preparation
stages P1, P5 and P6 may start; P2–P4 are scoped once D0 is approved.

Written 2026-11-08 against `main` @ `fd82d76`.

## Goal

1. **Reduce the number of dialogs.** Work that today opens a modal window
   (often on top of another modal) should happen in place, in the main window.
2. **One consistent workflow across the app.** The same task looks and works
   the same wherever it appears, so this touches every area: Observations,
   Measure, Analysis, Live Lab, Camera Import, Calibration, references,
   publishing and preferences.

Agents verify each stage themselves: behavior through tests, layout through
inspected screenshots. The owner approves the concept and each design
contract up front and gives a layout opinion on each finished stage (see the
verification tiers in `AGENTS.md`).

Out of scope: sync behavior, SQLite schema, the cloud contract, and
taxonomy data. A stage that finds it needs one of these stops and reports.

## Owner decisions (2026-11-08)

1. **Scope:** dialog reduction first, then a consistent workflow; expected to
   touch all areas of the app.
2. **Themes:** keep Clinical Nocturne (dark) and Slate Lab (light) from
   `ui/styles.py`.
3. **Mockups:** agents draft them; the owner reacts and edits.
4. **Current-state screenshots:** the owner's captures are in
   `docs/plans/november-ui/` (gitignored, local only; they contain personal
   profile data and paths, so never commit or publish them).

## Current state (2026-11-08)

### Dialog inventory

- 34 `QDialog` subclasses in `ui/`, plus 17 inline `QDialog(...)` builds.
- 176 `QMessageBox` calls (112 in `ui/main_window.py`, 30 in
  `ui/calibration_dialog.py`) and 35 `QFileDialog`/`QInputDialog` uses.
- Workflow dialogs (full windows for core work): `ObservationDetailsDialog`
  (Rediger observasjon), `ImageImportDialog` (Legg til bilder / Prepare
  Images), `CalibrationDialog`, `SpeciesPlateDialog`, `AddReferenceDialog`,
  `ReferenceLibraryManagerDialog`, `CloudConflictDialog`, `CloudSyncDialog`,
  `PortableImportDialog`, `SettingsHubDialog` (Preferanser) and its pages.
- Small form dialogs: `NewObjectiveDialog`, `ObservationSelectionDialog`,
  `RenameObservationDialog`, `ScaleBarImportDialog`, `ScaleBarCalibrationDialog`,
  `SyncShotDialog`, the three `Export*Dialog`s, `DatabaseBundleOptionsDialog`,
  the reference-library sub-forms, `MySharedReferencesDialog`,
  `SharedReferenceCatalogueDialog`, `LoadingDialog`.
- Legacy dialogs still referenced in `ui/main_window.py`:
  `ReferenceAddDialog`, `ReferenceValuesDialog`, `CloudReferenceDialog`,
  `ReferenceLibraryAttachDialog`. Check which are still reachable; dead ones
  are removed in their area's stage.

### What the screenshots show

- **Stacked modals.** Edit observation is a full-window modal; its
  “Rediger bilder (⌘E)” opens Prepare Images as a second modal on top.
  Add Reference and the Reference library open as modals over Analysis;
  Calibration and Preferences are separate windows.
- **The same controls built several times.** Slide/prep (mount, stain,
  condition, source), microscope (objective, contrast) and RAW processing
  appear in Prepare Images and again in Live Lab, with different layouts and
  label languages. Measure shows the same tags as chips over the image.
- **The image strip** appears on Observations, Edit observation, Prepare
  Images, Measure, Live Lab and Analysis (as a spore gallery), with different
  sizes, badges and selection behavior.
- **Three ways to show status:** the bottom status bar (red/blue/green),
  inline coloured labels, and message boxes.
- **Untranslated English** in a Norwegian UI: Live Lab (“CURRENT
  OBSERVATION”, “Start Session”, “Use for calibration”), Camera Import
  (“MICROSCOPE SESSIONS”, “Browse”, “Scan folder”, actions), Observations
  (“Show thumbnail”), Preferences (“Bright cutoff”), Reference library
  (“Close”, the missing-metadata warning), thumbnails (“From raw”,
  “Preview”).

## Stage D0 — Workflow and dialog-reduction concept

The first owner-facing stage; it decides what the later stages build.
Read-only for production code.

- **Workflow map:** the owner's main tasks end to end (new observation from
  the field, microscope session in Live Lab, camera-folder import, measure,
  analyse and compare with references, publish, sync and conflicts,
  calibration, preferences), and every dialog and message box each one
  passes through today.
- **Classify every dialog:** *inline* (becomes a panel or page in the main
  window), *sheet* (a lightweight in-window overlay for a short focused
  task), *keep* (a real modal: destructive confirmation, OS file picker,
  sync conflict resolution), or *remove* (dead or redundant). Message boxes:
  information becomes a non-blocking notice in one shared status area;
  confirmations stay only for destructive or irreversible actions, and
  cheap actions get undo instead.
- **Shared components** to build once and reuse: the image strip, the
  slide/prep and microscope tag controls, the RAW processing panel, the
  status/notice area, section headers and the action bar.
- **Mockups:** two or three alternative layouts for the main window and the
  observation workflow (for example: observation workspace with inline
  stages, or inspector side panel), as static HTML mockups using the theme
  colors from `ui/styles.py`, rendered to PNG. Publish them as a private
  artifact for the owner to compare and annotate. The chosen concept is
  saved as `docs/design/CONCEPT-desktop-workflow.md` with its images.
- **Output:** the approved concept, the dialog classification table, the
  list of shared components, and the order of the area stages below.

## Blockers: preparation stages

Each is its own PR. P1, P5 and P6 can run alongside D0. P2–P4 apply to the
areas the approved concept changes first.

### P1 — Style tokens

Colors and sizes are spread through the UI code: 576 hex colors outside
`ui/styles.py`, 225 `setStyleSheet` calls (67 in `ui/main_window.py`), and 103
`setFixed*` calls. A new look should be a change to named tokens in one
place.

- Define named color, spacing and radius tokens in `ui/styles.py` for both
  themes, and route the existing global stylesheet through them.
- Move inline colors to tokens screen by screen as each screen's stage
  touches it; P1 does only the token layer and the worst global offenders.
  This absorbs the INBOX item "continue the Slate Lab / Clinical Nocturne
  migration … consolidate remaining inline style sheets".
- Verification: renderer scenarios for both themes before and after are
  visually identical except where intended.

### P2 — Characterization tests for the areas being changed

Only 1 of 315 test files uses pytest-qt's `qtbot`. Before a screen is
redesigned, pin down how it behaves today: selection, editing, saving,
refresh after sync, keyboard shortcuts, and splitter/geometry persistence
across a restart. Drive real production widgets; mock only what the claim
does not depend on. The screen docs in `docs/` (`analysis-tab.md`,
`live-lab-tab.md`, `observation-dialog.md`, `prepare-images-dialog.md`,
`calibration-dialog.md`, `species-plate-dialog.md`, `reference-data-dialog.md`)
describe the intended behavior to test against.

### P3 — Screenshot scenarios and renderer speed

- Existing groups: conflict, measure, observations (taxon picker only),
  portable-import, raw-processing, reference-library, taxon-picker. No
  scenarios exist for the main window shell, the Observations list, Live Lab,
  Ingestion Hub, Calibration, Prepare Images or the Species plate. Add the
  main states of the areas being changed, plus dark theme and `de_DE`, and render a
  baseline set for the before/after comparison.
- The renderer is slow: the 77-scenario reference-library group takes about
  3 min 20 s (about 2.6 s per scenario, CPU-bound), and
  `test_default_renderer_emits_central_manifest_and_all_established_states`
  times out at its 120 s limit and is excluded from CI
  (`.github/workflows/ci.yml`). Profile the per-scenario cost (likely
  repeated database or taxonomy setup), and fix or rescope that test so the
  full renderer runs in CI.

### P4 — Extract in-scope panels from the monoliths

`ui/main_window.py` (23k lines) and `ui/observations_tab.py` (22.6k) can't be
read whole by an agent, which makes redesigning them risky. For each in-scope
panel that lives there, first move it into its own module without changing
behavior, as its own PR, with P2 tests green before and after. Follow the
`cloud_sync.py` extraction pattern (facade imports kept, mechanical moves).
Do not combine an extraction with a redesign in one PR.

### P5 — Shared macOS real-input helper

`tools/review_ui/taxon_picker_mac_check.py` has the window activation, key
posting (`CGEventPostToPid`) and mouse posting (NSEvent) built in. Turn
these into a reusable helper under `tools/review_ui/` so any screen's popup,
focus and keyboard behavior can be checked on cocoa. Keep the safety rules in
`.claude/rules/ui-screenshots.md`.

### P6 — Test environment

`pytest-qt` is pinned in CI (`pytest-qt==4.5.0`) but listed in no
requirements file, so a fresh `.venv` errors on the missing `qapp` fixture.
Add a test requirements file (or document the install). This is a test
dependency, not a production one; confirm with the owner before installing
anything into `.venv`.

## How to organize the work

- **Order:** D0 concept → shared components (built and tested once, before
  the areas that use them) → area stages in the order D0 sets. Dialog
  removal happens inside each area's stage: an area is done when its
  dialogs are inline, sheets or deliberately kept.
- **One area per stage, one stage per PR, landed on `main`.** The owner is
  the only desktop user, so no feature flags or long-lived redesign branch.
  If an area is too big for one PR (the observation workflow), split it into
  stages that each leave the app usable.
- **Each area stage:**
  1. A design contract in `docs/design/DESIGN-CONTRACT-<screen>.md`: mockup,
     layout rules, states, requirements, and which backlog items it
     includes. The owner approves it before code (the Add Reference contract
     is the model).
  2. Implement against the contract, reusing existing widgets and code
     paths. Remove `setFixed*` sizing on the touched widgets.
  3. Verify: P2 tests still green or deliberately updated, new behavior
     tests, scenarios updated, screenshots inspected (small and large window,
     dark theme, `de_DE`).
  4. Translations once per stage (`tools/update_translations.sh`), not per commit.
  5. Codex review, then a PR with before/after screenshots.
  6. The owner gives a layout opinion; changes are new commits; then merge.
- **Bugs in screens due for redesign:** fix them inside that screen's stage
  unless they block daily use, so the same code isn't changed twice.
- **Larger stages** (main window shell, Observations tab) can run as an Agent
  Sparring plan; smaller ones as ordinary tasks.
- **Record progress** in this plan's per-screen section as each stage lands
  (PR number and merge commit).

## Screen backlog (from INBOX, 2026-11-08)

Items moved here from `INBOX.md`, plus defects seen in the owner's
screenshots. Marked *bug* or *feature*.

### Seen in the 2026-11-08 screenshots
- *bug* The new-observation button reads “+ + Ny (N)” (icon plus a literal “+”).
- *bug* Prepare Images shows two “Lukk” buttons in its footer.
- *bug* Preferences → Online publishing: the “Spør før observasjoner …
  gjøres offentlige” checkbox overlaps “Inkluder vannmerke”.
- *bug* Reference library: the publication list's labels wrap and the year
  column is misaligned with its rows.
- *bug* Untranslated English strings (listed under *Current state*); fix in
  each area's stage and run the translation update.

### Analysis
- *feature* Default “Orient” and “Uniform scale” to on.
- *feature* Cmd/Ctrl-click additive selection and histogram additive selection.
- *feature* Consider renaming “Reference shape” to “Shape”.
- *feature* Make cloud-origin imported reference sources more visually
  distinct in the reference panel.

### Measure
- *bug* Make room for the measure-type radio-button labels.
- *feature* Hint bar at the bottom of Measure.
- *feature* Fine-tuning for multi-line measurement segments.
- *bug* Hide the “stain” pill on field images in Measure and observation
  main-image surfaces.
- *bug* Local database values must take priority over file EXIF in the Measure
  Info box (verify first; same check for Prepare Images).

### Observations tab and main window
- *bug* Selection/highlight artifacts in AI suggestions and the Observations
  table; align them with the Measurements table.
- *feature* The main window still uses the four-field `TaxonInputController`
  (`ui/main_window.py`, also `ui/cloud_reference_dialog.py`); the observation
  editor uses the unified taxon picker (PR #47). Decide whether these move to
  the unified picker (design decisions in
  `docs/plans/completed/2026-10-27-unified-taxon-picker.md`).
- *bug* Quitting after an aborted sync crashed with `QThread: Destroyed while
  thread 'Artsobs mobile link check' is still running` (2026-10-19). A guard
  exists near that worker in `ui/observations_tab.py`
  (test in `tests/test_observations_tab_cloud_sync.py`); this path escapes it.

### Prepare Images
- *bug* Editing a raw image and then zooming can revert the preview to the old JPEG.
- *bug* Thumbnails do not update after edits.
- *bug* The cloud-synced image warning overlay is wrong.
- *bug* Android-imported JPEG portrait rotation in thumbnails (and Measure).
- *feature* Adjustable thumbnail gallery height; no cropped/hidden
  thumbnails; thumbnails around 100 px.
- *feature* Multi-select auto white balance, calculated per image.
- *feature* Apply one custom/picked white balance to all selected images.

### Live Lab
- *bug* After editing a “from raw” image, the main preview stays stale until
  another image is selected.

### Ingestion Hub (Camera Import)
- *bug* Naming: fix “Intestion”, rename “Sync shot” to “Camera time offset”
  and “Microscope sessions” to “Live lab sessions”.
- *feature* Layout order: Import folder, Camera time offset, Live lab
  sessions, Actions; update button hints.
- *feature* Richer manual reassignment for unmatched images (also in
  `docs/hardware-sync.md`).

### Species plate / publishing
- *bug* The publication plate must respect disabled image bubbles.

### Cloud and account surfaces
- *bug* Replace “Unable to save cloud login” for account-link protection
  failures with wording that names the account mismatch.
- *bug* The account-mismatch message tells users to reset the cloud link,
  but no such tool exists: add a real Reset Cloud Link tool or remove the
  instruction. (A real reset tool touches account binding: security review.)
- *feature* A non-modal pending-cloud-media indicator on close; no blocking dialog.

### Menus
- *feature* Menu link to Pro information/payment on `sporely.no`; no embedded checkout.
- *feature, deferred* Observer-safe `Help → Taxonomy` menu
  (`database/taxonomy/docs/ui-menu-recommendation.md`).
- *feature, proposed* “Search in Species Fungorum” action from the taxon
  detail UI; investigate a reliable deep link first (full notes in git
  history of `INBOX.md`, 2026-10-24).
