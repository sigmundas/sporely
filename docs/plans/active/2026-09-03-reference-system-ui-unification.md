# Reference-system UI unification (Sporely desktop)

Consolidate five parallel "add reference to the analysis plot" entry points into
**one comparison-centric panel** + **one tabbed picker dialog**. Match the two
approved mockups' structure/hierarchy (panel + picker), reusing existing widget
styles and the app's global tab style. Backend/model code stays; only UI entry
points are removed.

Status: Stages 4b (`c345ac8`) and 4b-fix (`14525ff`) are already committed.
Fresh top-level acceptance review on 2026-09-05: **4b-fix2 confirmed** within
its approved corrective scope. User confirmed all seven manual checks passed.
Stage 4c (Enter-manually tab) is described below; a second, later fresh
independent review confirmed it and it is now committed (see "Landed stages").
Approved plan stage 4 (Wire Community / My observations / Enter manually
tabs) is now fully landed across its 4a/4a-fix/4b/4b-fix2/4c sub-stages.
Stage 5 was re-scoped 2026-09-05 from "verdict computation" to "reference
context/provenance + badges + preview summary" (match/verdict work moved to
`docs/plans/active/2026-07-12-parmasto-matching-foundation.md`) — see the note
at the end of "Landed stages". A bounded stage-5 prompt now exists:
`.sparring/prompts/sporely-py/stage-5.md`.

## Current stage / handoff — 2026-09-05 (stage 4c implementation)

### Independent acceptance review — 2026-09-05 (fresh session, after manual testing)

**Confirmed and committed.** The user ran all seven manual checks in
`.sparring/prompts/sporely-py/stage-4c.md` and reported all passing,
including a deliberate different-species comparison (added a *Mycena
metata* reference to an observation identified as *Mycena leptocephala*;
the reference persisted as *Mycena metata* and the observation's own
identification was untouched). A fresh top-level review verified this
independently against the code rather than accepting the report as-is:

- Confirmed `ReferenceAddDialog` (`ui/main_window.py:5195`) is now a thin
  wrapper delegating to an embedded `ReferenceEntryEditor` via `__getattr__`
  — genuine reuse, not a parallel implementation.
- Confirmed `_submit_reference_editor_result` (`ui/main_window.py:12008`) is
  the single submission path for both the legacy Quick-add dialog and the
  new manual tab's `_add_manual_callback` (`ui/main_window.py:10063`).
- Confirmed the observation-drift/failure gate: `_add_manual_callback`
  returns `False` on a drifted observation, and
  `AddReferenceDialog._on_add_to_plot_clicked` (`ui/add_reference_dialog.py:1014`)
  only calls `accept()` when that callback reports success — matches
  `test_manual_tab_stays_open_when_callback_reports_failure`. `Cancel` only
  calls `reject()` (`ui/add_reference_dialog.py:341`), so cancellation never
  writes.
- Confirmed target-identity independence in
  `ReferenceEntryEditor.set_comparison_target`
  (`ui/reference_entry_editor.py:560`): resets publication/treatment
  identity and any built result on every target change, while preserving
  already-entered measurement values — and confirmed the deeper fix behind
  it, the new `observation_taxon_id` field
  (`ui/reference_entry_editor.py:1110`, read at
  `ui/main_window.py:11667`), which lets the drift guard compare the
  *observation's* taxon instead of being confused by the *comparison
  target's* taxon — exactly the persistence-contract blocker this stage's
  prompt required a fix for.
- Reran the full required command set: `test_add_reference_dialog.py` +
  `test_reference_entry_editor.py` (58 passed), the second regression batch
  (64 passed), `test_render_review_screenshots.py` (12/12 — the three
  previously-tracked stale inventory assertions are now fixed, as the
  prompt required), `py_compile` clean, `git diff --check` clean, zero
  unfinished translation entries in all three languages. Visually inspected
  the manual-tab Norwegian and invalid-input screenshots: no clipping, tabs
  and controls fully accessible, footer state correctly reflects validity.
- **UX finding confirmed, correctly scoped as a follow-up, not a blocker.**
  The confirm dialog at `_persist_normalized_reference_from_dialog`
  (`ui/main_window.py:11741`) reads "The species entered in the panel
  ({panel}) differs from the observation's taxon record ({observation})...
  If this is an accidental edit, click No" — wording written for the
  legacy panel's editable identification fields. It now also fires for the
  Enter-manually tab whenever the chosen comparison target's genus/species
  differ from the observation's own, which is the deliberately common case
  for a cross-species literature comparison, not a probable mistake. This
  guard is pre-existing and unrelated to this stage's edits (untouched by
  the taxon-ID separation fix above); the implementer's own report already
  named this exact deviation and deferred it (see "Known issues" and the
  entry below). No code changes were made in this review session per the
  user's request to scope this session to verification only.

Commit: **see hash below** (created immediately after this entry).
Archived prompt: `.sparring/prompts/sporely-py/completed/stage-4c.md`.

### Stage 4c — Enter-manually tab wired; human-gated, uncommitted

Resolves the persistence-contract blocker recorded below (see "Stage 4c
boundary inspection") by threading a second, dedicated identity field
through the existing submission host, rather than reusing
`sporely_taxon_id` for two different purposes.

**Extraction.** `ReferenceAddDialog`'s ~1500-line body (`ui/main_window.py`,
previously ~L5341–6867) is split into a new module,
`ui/reference_entry_editor.py`:
- `ReferenceEntryEditor(QWidget)` owns the paste/parse min-max table, spore
  table, Parmasto fields, publication picker, and the existing/new-data
  Data section — every method verbatim except `_on_save` (renamed
  `validate_and_build_result() -> bool`, no `accept()`) and the new methods
  below. No parser/validation/normalized-payload logic was duplicated.
- `SporeDataTable` and `_PublicationSearchProxyModel` moved into the same
  module (only ever used by this editor); `main_window.py` imports both
  back for backward compatibility.
- `ReferenceAddDialog` (`ui/main_window.py`) is now a ~120-line modal
  wrapper: sci-name header + Save/Delete/Cancel chrome around one embedded
  `ReferenceEntryEditor`, forwarding every accessor
  (`result_data`, `pending_reference_work`, `quick_add_treatment_payload`,
  `selected_measurement_set_id`, `is_use_existing_set`,
  `normalized_measurement_set_payload`) to it, plus a `__getattr__`
  fallback to the embedded editor for any other attribute (so existing
  tests reaching into `dialog.minmax_table` / `.spore_table` /
  `.publication_combo` etc. keep working unchanged).

**Target identity vs. observation identity (the blocker's fix).**
`ReferenceEntryEditor` now carries two taxon-id fields instead of one:
`sporely_taxon_id` (the *currently selected comparison target's* id —
may legitimately differ from the observation) and `observation_taxon_id`
(the observation's own, fixed for the editor's lifetime). Both land on the
built payload. `MainWindow._persist_normalized_reference_from_dialog`
(~L11578) reads `payload.get("observation_taxon_id", payload.get(
"sporely_taxon_id"))` for its drift guard — comparing the *observation's*
taxon, not the target's — while the treatment-creation branch still reads
`sporely_taxon_id` (the target's id) unchanged. Callers that predate the
picker's target selector (the legacy Quick-add dialog) never set the new
field, so the fallback preserves their behavior exactly: no schema or
persistence change, one new optional payload key.

**Manual tab wiring** (`ui/add_reference_dialog.py`):
- `_build_manual_tab` embeds `ReferenceEntryEditor` (no modal chrome),
  sharing the dialog's one `ReferencePreviewPane` instance and its
  Add-to-plot/Cancel footer, replacing the stage-3/4a/4b stub pane.
- `_apply_taxon_target` (the existing "Compare against" handler) now also
  calls `ReferenceEntryEditor.set_comparison_target(...)`, which resets
  publication/treatment identity (selected work, pending new work, the
  publication combo, `name_as_published_input`, and any already-built
  result) for the new target while preserving already-entered measurement
  values (min/max, spore points, Parmasto) — literature data independent
  of which taxon it is compared against.
- `_update_footer_state`/`_on_tab_changed` gained a manual-tab branch:
  footer enablement reads `ReferenceEntryEditor.is_ready_to_submit()`
  (mirrors `validate_and_build_result()`'s branching without popping
  dialogs); revisiting the tab calls the editor's `sync_preview()`.
- `_on_add_to_plot_clicked`'s manual branch is the one tab that does NOT
  unconditionally `accept()`: it validates first, then only accepts if
  `manual_attach_callback(editor)` returns `True` — the other three tabs'
  callbacks have no failure signal, but a manual submission can still fail
  the observation-drift check after the click, and per the prompt must
  leave the picker open rather than claim success.

**Shared submission routine** (`ui/main_window.py`): extracted
`_submit_reference_editor_result(editor, *, sync_panel)` from the body of
`_on_reference_panel_add_clicked` (quick-add-first ordering, legacy write,
normalized attach, `reference_series` fallback, all unchanged) — the
`editor` argument is duck-typed, so both the legacy `ReferenceAddDialog`
wrapper and a bare `ReferenceEntryEditor` from the picker satisfy it.
`_on_reference_panel_add_clicked` now just builds the dialog and calls the
shared routine (`sync_panel=True`, to keep syncing the old panel's own
Source dropdown). `_on_add_reference_clicked`'s new `_add_manual_callback`
re-validates the captured observation (mirroring `_add_callback`'s
existing guard for Library/My-observations), then calls the same routine
(`sync_panel=False`) and returns its success as the accept/stay-open
signal above.

**Live preview.** `ReferenceEntryEditor._refresh_preview()` pushes the
current tab's data into the shared `ReferencePreviewPane` on every
relevant change (min/max edits, parse, spore-table edits, Parmasto edits,
existing-set selection, the Data-section radio, the editor's own inner
tab change, and `set_comparison_target`) and on `sync_preview()`. The
range-preview bound/mean values fall back from the extreme columns to the
typical columns (mirroring `normalized_measurement_set_payload`'s existing
Q-column fallback) so a bare "9.8–11.3" range with no parenthesised
extremes still shows in the preview instead of "—".

**Translation-context preservation.** Every `self.tr(...)` call moved into
`ReferenceEntryEditor` was changed to
`QCoreApplication.translate("ReferenceAddDialog", ...)` so the ~80 moved
strings keep resolving against the existing "ReferenceAddDialog"-context
catalogue instead of a new "ReferenceEntryEditor" context lupdate would
otherwise infer from the class rename (the same class of bug fix2 already
made for `cloud_reference_dialog.py`/`add_reference_dialog.py`).
`SporeDataTable`'s own `self.tr(...)` calls were left untouched (its class
name is unchanged, so its existing "SporeDataTable" context is unaffected
by the file move). `ui/reference_entry_editor.py` was added to
`tools/update_translations.sh`'s source list. `./tools/update_translations.sh`
found 8 new source strings (the preview feature is new; everything else
matched the preserved context) — 4 were resolved by the same-text
heuristic against identical strings elsewhere in the catalogue, and 4
("Existing measurement set", "Manual entry", "Not applicable: entered
manually.", "Manually entered") needed fresh translations, added by hand
for nb_NO/sv_SE/de_DE (German informal "du" — none of the four needed a
"du" form). Final extraction: 2325 finished translations per language,
zero unfinished.

**Regression tests** (in addition to the required commands below):
`tests/test_reference_entry_editor.py` (new, 11 tests) — valid
range/points/existing-set submission and payload shape (both taxon-id
fields present), empty/malformed-input fail-closed, the "no publication
selected" confirm gate's Yes/No paths, `set_comparison_target` invalidating
publication/work state while preserving measurement values (and failing
soft on a non-numeric target id), and preview-pane sync/clear. Extended
`tests/test_add_reference_dialog.py` (+13 tests): manual-tab footer
enable/disable, accept-only-on-callback-success vs. stay-open-on-failure,
target-change resetting the manual editor, tab-revisit preview resync, and
two host-level tests (`_add_manual_callback` routes through
`_submit_reference_editor_result` with `sync_panel=False`; rejects and
never calls it when the observation drifted). The three pre-existing stale
renderer-inventory assertions in `tests/test_render_review_screenshots.py`
(an entirely unrelated `portable-import` scenario group, and the ten
already-registered `reference.*` scenarios stage 4b-fix2 added) are now
fixed as part of the required inventory update for this stage's new
scenarios — all three assertions pass; the full renderer test module and
this stage's new manual/picker tests both pass with no remaining test
debt.

**Renderer scenarios** (`tools/review_ui/scenarios/references.py`): seven
new registered scenarios — `reference.add-dialog-manual-{range,points,
invalid}` in light and dark, plus `reference.add-dialog-manual-nb-no` — a
selected publication with a realistic parsed range (both extreme and
typical bounds, explicit Qm), eight raw paired spore points, and the
empty/invalid state, in both themes; the Norwegian case additionally
exercises the real translator end-to-end.

**Validation run:**
```
./.venv/bin/pytest tests/test_add_reference_dialog.py tests/test_community_results_pane_requests.py tests/test_comparison_panel_model.py tests/test_comparison_list_widget_rebuild.py tests/test_reference_series_palette.py tests/test_reference_add_dialog_focus.py tests/test_reference_add_dialog_normalized.py tests/test_reference_panel_coordinator_existing_set.py tests/test_reference_panel_mainwindow_e2e.py tests/test_reference_attach_persistence_e2e.py tests/test_render_review_screenshots.py tests/test_reference_entry_editor.py -q
```
→ **134 passed** (includes 47 in `test_add_reference_dialog.py`, up from
32 before this stage; 11 new in `test_reference_entry_editor.py`; 12 in
the renderer module, all three previously-stale inventory assertions now
passing). `py_compile` of `ui/main_window.py`, `ui/add_reference_dialog.py`,
`ui/reference_entry_editor.py`, `tools/review_ui/scenarios/references.py`
passed. `./tools/update_translations.sh` passed (2325/2325 finished, all
three languages). `git diff --check` passed. Reference-library renderer
group: **47 screenshots**, manifest at
`/private/tmp/sporely-stage-4c-review/manifest.json` (the seven new
manual-tab scenarios were additionally re-rendered twice more during
iteration, at `/private/tmp/sporely-stage-4c-review2/` and `.../review3/`,
to verify the preview-fallback and translation-context fixes below).

Inspected screenshots (all seven new scenarios, both themes where
applicable):
- `reference.add-dialog-manual-range(-dark)`: publication selected, parsed
  range table populated, shared preview's Length/Width/Q rows match the
  entered min/typical/max/mean exactly (Q fallback confirmed: 1.70/1.89/
  2.10), Add to plot enabled, all four source tabs and five preview
  sub-tabs visible with no scroll arrows at the natural 1400×760 size.
- `reference.add-dialog-manual-points(-dark)`: eight raw paired points
  populate both the Spore-data table and the shared preview (computed
  min/mean/max per dimension, "n = 8 spore measurements"), Add to plot
  enabled.
- `reference.add-dialog-manual-invalid(-dark)`: empty min/max table, empty
  preview ("No dataset selected" / all "—"), Add to plot correctly
  disabled, in both themes.
- `reference.add-dialog-manual-nb-no`: real Norwegian catalogue resolves
  every label on this tab — tab name "Skriv inn manuelt", "Publikasjon",
  "Navn som publisert", "Lokator", "Bruk eksisterende målesett"/"Legg inn
  nye data", "Min/maks"/"Sporedata"/"Parmasto Biometrics", "Tolk"/"Bytt
  L↔B", the five min-max column headers, "Tolket — se gjennom og rediger
  før lagring.", footer "Avbryt"/"Legg til i diagram" — confirming the
  translation-context preservation fix actually resolves at runtime, not
  just compiles.

**Known issue found, not fixed (out of this stage's scope):** the
Norwegian scenario is the first to show a populated `ReferencePreviewPane`
summary table in Norwegian (no prior nb-no scenario exercised it — the old
Quick-add dialog never used `ReferencePreviewPane`). Its "Median / Mean"
column header ("Median / Gjennomsnitt" in Norwegian) is long enough to
clip on both edges without an ellipsis at this viewport. This is the same
class of defect as the already-tracked "Known issues" meta-line
truncation from stage 1 (elide + tooltip needed on `ReferencePreviewPane`
itself, matching `ComparisonRowWidget`'s existing elision pattern) — it is
shared infrastructure across all four picker tabs, not something this
stage's editor relocation introduced or is in scope to redesign. Added to
the Known issues list below for a future polish stage.

**Deviations from the prompt:** none identified; the confirm-dialog for
"the panel genus/species differs from the observation's taxon record"
(`_persist_normalized_reference_from_dialog`, ~L13241) was deliberately
left unchanged even though it will now fire routinely whenever the
selected comparison target's genus/species differ from the observation's
own (previously an anomaly, now an expected case for an AI/typed target).
Changing that gate's condition was judged out of scope: it is existing
host logic shared with the legacy caller, and the prompt asked only to
separate identity fields for the drift guard, not to redesign this
confirmation. Noted for a future bounded stage if the resulting
confirmation frequency proves to be a real UX problem.

**Verification tier: human-gated, uncommitted.** All agent-verifiable
checks above pass. Do not commit until the user confirms the seven manual
checks in `.sparring/prompts/sporely-py/stage-4c.md` (open Analysis → Add
reference → Enter manually; valid range; raw points + legacy-only form;
empty/malformed input; target/tab switching and cancellation; publication-
backed entry surviving reopen; resize/keyboard/light+dark+language
smoke-test of Library/Community/My observations/old Quick add). Do not
archive the stage-4c prompt until after that acceptance and the subsequent
commit.

### Stage 4c boundary inspection — blocked before implementation

Selected prompt: `/Users/sigmundas/Documents/Code/sporely/.sparring/prompts/sporely-py/stage-4c.md`.
No production files were changed. The pre-existing AGENTS.md edit is preserved.

Existing reuse contract inspected:
- `ReferenceAddDialog._on_save` (`ui/main_window.py:6087`) validates existing-set
  selection or `_reference_record_data` / `_points_data`, captures publication
  and observation/taxon context, and requests explicit legacy-only confirmation
  where applicable. `result_data`, `quick_add_treatment_payload`,
  `pending_reference_work`, `selected_measurement_set_id` and
  `normalized_measurement_set_payload` provide the host result boundary.
- `MainWindow._on_reference_panel_add_clicked` (`ui/main_window.py:13516`)
  preserves normalized-first quick-add, legacy persistence, existing-set
  attachment and suppression of a duplicate legacy plot series.

Concrete blocker: `_persist_normalized_reference_from_dialog` compares the
submitted `sporely_taxon_id` to the receiving observation's live taxon ID
(`ui/main_window.py:13175–13194`). An intentionally different comparison
target with its own ID is rejected as observation drift. A name-only AI/typed
target avoids that ID comparison but reaches the observation-name comparison
and synonym/historical-name confirmation (`ui/main_window.py:13241–13266`).
Thus the existing host contract conflates comparison identity with observation
identity; merely relocating the editor cannot provide the requested independent
target behavior while preserving that contract. Borrowing the observation ID
or silently bypassing its guard would violate the prompt's identity requirement.

Proposed bounded follow-up: explicitly authorize separating captured receiving-
observation identity (used for drift validation before every write) from the
selected comparison target identity (used for treatment creation). Preserve
the old caller's drift/synonym semantics; specify name-only target handling
using the existing nullable treatment taxon ID, without a schema change. Add
focused rejection/acceptance tests for differing target IDs, name-only targets,
observation drift and unchanged identification before resuming editor relocation.

Verification: selector chose sporely-py/stage-4c.md; `git status --short`
showed only the unrelated AGENTS.md edit before this handoff; base commit
`3d81ace` was confirmed with `git show --stat`. Targeted source inspection
established the blocker. No implementation tests, translations or renderer
captures were run because no implementation was made. `git diff --check`
passed for this documentation update.

Status: blocked at the prompt's explicit persistence-contract stop condition;
uncommitted, no stage commit, prompt remains pending and unarchived. All editor,
callback extraction, regression and screenshot work is deferred. Human-gated
acceptance checks 1–7 in the selected prompt remain pending after implementation;
there is no new interactive behavior to test in this pass.

**4b-fix2 landed: `3d81ace`.** Independent review confirmed the corrective
scope and the user confirmed all seven manual checks passed. The completed
prompt is archived at
`../.sparring/prompts/sporely-py/completed/stage-4b-fix2.md` from the repo root.
Next pending prompt: `../.sparring/prompts/sporely-py/stage-4c.md`.
Approved plan stage 4 supports continuing to Enter manually by reusing the
existing editor/host submission behavior. The new prompt preserves persistence
and existing callback paths, stops on a necessary contract change, and is
human-gated. No 4c implementation occurred in this acceptance session.
Only the unrelated pre-existing AGENTS.md change is left outside the commits.


### 4b-fix2 correction — independently reviewed and manually accepted

Completed prompt:
`/Users/sigmundas/Documents/Code/sporely/.sparring/prompts/sporely-py/completed/stage-4b-fix2.md`.
Implementation reviewed against base `14525ff91f6b7ada1f899c03852f54eb065b1fe1`.
All seven manual checks passed per the user in this acceptance session.
The user authorized committing this verified correction and archiving its prompt.
No 4c implementation was performed.

This pass began with partial corrective code/tests, renderer plumbing, and
TS/QM edits already in the working tree, in addition to AGENTS.md and the
review/handoff edits. Those edits were preserved and the correction completed
in place. The older claim that the working tree contains only a handoff edit
is historical, not the present state.

Corrected scope and reuse:
- `ui/cloud_reference_dialog.py`, `CommunityResultsPane` (~1202–1570): request
  generations in `refresh` and `_on_result_selection_changed` guard search and
  detail success/error callbacks. Superseded requests remain in `_worker_refs`
  until the existing `_release_worker` path releases them. `closeEvent`
  invalidates callbacks and waits for all tracked requests, including superseded
  ones. Target changes themselves do not wait. Existing `_CloudSearchWorker`,
  `_CloudDetailWorker`, preview helpers and `current_mode_payload` are reused;
  adapters, network contracts and persistence are unchanged.
- `ui/add_reference_dialog.py`, `__init__` (~358–360) and
  `_on_taxon_target_text_entered` (~488–517): dialog `finished` closes the
  Community pane; unchanged selected labels reuse structured item data through
  `_apply_taxon_target`. Deliberately edited text retains the existing parser.
- `ui/main_window.py`, `_on_add_reference_clicked` (~11539–11559) and
  `_active_sporely_taxon_id` (~13101–13129): one captured observation snapshot
  supplies name and ID. The existing ID helper accepts an optional snapshot;
  existing callers still fetch the active observation. Reused
  `ObservationDB.get_observation`, `_clean_ref_genus_text`,
  `_clean_ref_species_text`; existing drift guards and attach closures remain.
- Both picker/pane modules use explicit `QCoreApplication.translate` contexts
  for their own strings: lupdate had extracted these into the empty context,
  where Qt could not find them at runtime. The Norwegian popup screenshot
  exposed this despite successful QM compilation.
- `tests/test_add_reference_dialog.py`: isolated settings and preview stubs,
  actual Qt keyboard selection/Return, structured own-target/free-text tests,
  synthetic host observations and a write-rejecting persistence stub. Removed
  the incoming host tests' real SQLite-row setup. Existing filter/attach tests
  are retained. `tests/test_community_results_pane_requests.py`: manually driven
  fake workers, A→B→C ordering, late success/error, selection/clear invalidation,
  worker release, post-close callbacks and rejecting the owning picker.
- `tools/review_ui/registry.py` (`ReviewScenario`) and `runner.py` (`_capture`):
  bounded generic options preserve builder sizing and capture a post-show
  surface such as the real combo popup. `tests/test_render_review_screenshots.py`
  adds two focused tests for those facilities.
- `tools/review_ui/scenarios/references.py`: `_fixture` isolates geometry and
  splitter QSettings per builder; existing suppressed/colour/default/selector
  builders are reused, with both-theme and Norwegian cases. Default sizing
  skips viewport resize/adjustSize. Popup evidence captures the actual separate
  popup surface, not a closed combo or a composite desktop screenshot.
- `tools/update_translations.sh` includes comparison_panel, cloud_reference_dialog
  and reference_preview_pane. Paired `i18n/Sporely_{nb_NO,sv_SE,de_DE}.{ts,qm}`
  refreshed, preserving incoming translations and correcting context placement.
  Final extraction: 2318 finished translations per language, zero unfinished;
  no unrelated unfinished entries remain to list. No backlog sweep was performed.
- This canonical plan updated; pre-existing `AGENTS.md` edit left untouched.
  No changes to comparison_panel production logic in this correction.

Verification:
- Pre-fix reproduction loaded exactly the three UI modules from `14525ff` into
  an isolated Python process, then ran the corrected no-database test fixtures.
  **11 failed, 33 passed**: AI Return, own Return, all three mismatched-host
  cases, skipped B/C searches, stale search/detail success/error, clear-target
  detail, and outstanding-worker cleanup. Source tree was never reverted.
  Log: `/private/tmp/sporely-fix2-baseline.log`; harness:
  `/private/tmp/sporely-fix2-baseline.py`. Two further close regressions were
  added after this baseline run. Earlier incoming-fixture runs hit unintended
  database access; these are not counted as behavioral reproduction evidence.
- Required focused command (dialog, Community requests, comparison model,
  comparison rebuild, palette): **62 passed in 9.39s**. Log:
  `/private/tmp/sporely-fix2-tests.log`.
- New renderer tests only: **2 passed, 10 deselected in 0.83s** via
  `pytest tests/test_render_review_screenshots.py -q -k
  'test_capture_natural_size or test_capture_open_popup'`.
- Required `py_compile` for five named modules, plus touched renderer/test
  Python files: passed. `git diff --check`: passed.
- `./tools/update_translations.sh`: passed; TS/QM context fix checked through
  actual Norwegian renderer output. No production dependencies or heavy builds.
- `python -m tools.render_review_screenshots --list`: passed. Required
  reference-library group: **40 screenshots**, manifest:
  `/private/tmp/sporely-stage-4b-fix2-review/manifest.json`.

Inspected manifest-listed evidence (12 scoped images; remaining group images
were generated but are not claimed as individually reviewed):
- `reference.fix2-suppressed-{light,dark}.png`: observation n=6 cystidia,
  dim unchecked reference rows, bottom spore-only hint visible.
- `reference.fix2-colors-{light,dark}.png`: blue/orange/yellow chips are distinct.
- `reference.fix2-natural-{light,dark}.png`: actual initial **1209×438** capture,
  all four source and five preview tabs visible without arrow scrollers, with
  empty isolated geometry and splitter settings.
- `reference.fix2-popup-{light,dark}.png`: actual open popup (800×45), own entry,
  82%/41% candidates, long vernacular name and æøå visible without clipping.
- `reference.add-dialog-library{,-dark}.png`: unchecked taxon filter, each
  literature row identifies its taxon; long range text elides after the taxon.
- `reference.fix2-selector-nb-no.png` and `reference.fix2-hint-nb-no.png`:
  “Denne observasjonen” and “Referansedata gjelder sporemålinger.” load and fit.
  Fixture names/badges intentionally remain fixture text. Popup capture is of
  the popup alone; the natural-dialog captures establish the surrounding layout.
All paths above are relative to the manifest directory. Static evidence does
not establish interaction, hardware behavior, or persistence across restart.

Scope adjustments with reasons: the incoming partial host tests were replaced
because the prompt forbids database rows; explicit translation contexts were
necessary because translated selector evidence initially remained English;
shared renderer changes were limited to the sizing/popup facilities permitted
by the prompt. No persistence/adapter/callback abstraction or UI redesign.

Verification tier: **human-gated, now accepted**. Manual acceptance checklist —
all seven **PASS**, explicitly confirmed by the user on 2026-09-05:
1. With spore and cystidium measurements and an attached reference, switch
   Spores → cystidia → Spores. Count must follow population; references dim and
   uncheck outside Spores; original enabled states restore without flicker/loops.
2. Compare observation/reference chips to plotted Spores series. Colours must
   agree and be visibly distinct.
3. Inspect a Library row in the comparison list. Its second line must show the
   measurement range, not repeat the publication title.
4. Open Add reference: four source and five preview tabs must fit without
   arrows. Resize/move splitter, close/reopen, restart app/reopen. Geometry and
   splitter must persist and tabs must still fit.
5. Select AI targets by mouse and keyboard, press Return; type a non-AI taxon
   and Return. Switch targets rapidly during Community loading and inspect
   Library/Community/My observations. Title/filters must follow target; no
   percentage/vernacular leaks, unintended add, or obsolete results/details.
   Change the old panel target and reopen: own entry still identifies the
   observation consistently.
6. Uncheck Library's Only this taxon: every row must identify its taxon.
   Recheck: filtering must return to the selected target.
7. Reopen Edit Observation: identification unchanged — PASS.

Independent acceptance review verified the targeted production diff, baseline
harness/log (11 expected pre-fix failures), tests and renderer changes. The
required 62 corrective tests passed again; the combined run including the full
renderer module returned **71 passed, 3 failed in 36.26s**. All three failures
are stale scenario-inventory expectations in
`tests/test_render_review_screenshots.py` (lines 76, 85, 153), not capture
failures: they omit existing reference scenarios and the portable-import group.
The first two failures were reproduced with the reference registry loaded from
`14525ff` (30 reference scenarios versus 10 expected); the third asserts the
same outdated inventory after a successful default capture. The ten new scoped
scenarios increase that inventory but do not cause the underlying mismatch.
This is tracked test-maintenance debt, not a claim that the full suite is green.
The two new capture tests pass. No production correction was required by review.

Reviewer inspected all 12 scoped manifest images: natural-size tab bars fit in
both themes, popup contents/percentages/æøå are visible, suppressed rows and
colour distinctions are present, unfiltered rows identify taxa, and Norwegian
selector/hint translations load. Compiled catalogues for nb_NO/sv_SE/de_DE
resolve selector, Community status and comparison hint strings; all have zero
unfinished TS entries. Syntax checks for all touched Python files and
`git diff --check` passed. Pre-existing AGENTS.md edits are excluded from the
stage commit. No authoritative security boundary changed; security escalation
is not warranted.

Deferred unchanged: 4c/manual entry, verdict computation, old-control removal,
palette stability and callback unification. No fresh top-level final review
was substituted by a subagent.

### Incoming corrective handoff (preserved)

Next authorized implementation: **4b-fix2**, a bounded correction within the
approved 4b intent. Pending prompt:
`.sparring/prompts/sporely-py/stage-4b-fix2.md` (relative to the workspace parent).
It covers the three reviewed taxon-target blockers, failing-then-passing
regression tests, scoped translations, and missing meaningful static evidence.
The implementer updates this plan and stops after agent-verifiable checks,
leaving corrective work uncommitted and the seven manual checks pending for
the user. No 4c work is authorized. This review pass wrote only the corrective
prompt and this handoff; it did not implement the corrections.

### Prior handoff evidence

At review, HEAD is `14525ff91f6b7ada1f899c03852f54eb065b1fe1`. The prompt is
now `.sparring/prompts/sporely-py/completed/stage-4b-fix.md`; there were no
top-level pending prompts before 4b-fix2 was authored. The earlier selector
claim is stale.
This pass changes only this handoff; no production implementation was duplicated.
The pre-existing working-tree edit to `AGENTS.md` was preserved.

Verification rerun:
- Required comparison-model/dialog/widget tests: **46 passed in 3.26s**.
- `tests/test_reference_series_palette.py`: **2 passed in 0.60s**.
- Required `py_compile` of the four UI modules: passed.
- Repository reference-library renderer: 30 screenshots generated; manifest at
  `/private/tmp/sporely-stage-4b-fix-20260905/manifest.json`.
- Inspected suppressed list (n=6 cystidia, dim unchecked references and hint),
  color list (blue/orange/yellow), default-size dialog (four source and five
  preview tabs visible), selected AI target (82%), and dark Library with taxon
  filter unchecked (taxa visible). These are static evidence only.

Evidence limitations: the existing AI scenario captures a selected candidate,
not the open popup requested by the prompt; suppressed/color/default/AI scenarios
have no dedicated dark counterparts. The default-size scenario supplies a
1400x760 viewport, so the screenshot alone does not prove natural default sizing.
These gaps remain for the fresh review; do not represent this as full acceptance.

Existing paths confirmed: `MainWindow._reference_overlays_allowed_for_category`
is shared by plot and comparison list; `update_gallery` refreshes the list after
the plot (no new signal connection). `_collect_reference_ai_suggestions` reads
stored Artsorakel/iNaturalist predictions; `_on_ref_ai_suggestion_activated` is
the old panel fill path. `comparison_panel._format_detail` now uses library
kind/raw text like `AddReferenceDialog._add_candidate_item`; previously it fell
through to source text, duplicating the title's publication.

Manual status: all seven 4b-fix manual items are **not run in this pass**:
1. Switch Spores to cystidia and back; verify count and reference dimming restore.
2. Verify distinct observation/reference colors in chips and plot.
3. Verify library row detail is the measurement range.
4. Verify all tabs fit and dialog size persists after resize/close/reopen.
5. Select an AI target, then enter an arbitrary taxon; verify filter and title.
6. Uncheck Only this taxon; verify each row identifies its taxon.
7. Reopen Edit Observation; verify identification was unchanged.

Commit status: existing implementation remains at `14525ff`; this handoff is
uncommitted pending review of the outstanding evidence/manual gates. No push.
Deferred: stage 4c manual entry, stage 5 verdicts, stage 6 control removal and
palette stability. Suggested future callback unification: one typed request
dataclass with a source-kind discriminator and explicit local-set, observation,
cloud, or manual payload, handled by one dispatcher. Cost: adapt existing caller
closures and dialog submission paths, plus per-variant validation/tests; no
persistence change is inherently required. Not implemented in this stage.

## Independent review — 2026-09-05

Verdict: **partly confirmed**. No stage 4c prompt issued; no implementation edits.
`.sparring/latest.md` (Captured 2026-09-05 00:01 CEST) describes an unrelated
AGENTS.md scout, not the implementation. Review therefore used the committed
diff, completed prompt, and current handoff. The earlier backfill already
records stages 1–4a-fix; do not duplicate it based on the skill's stale note.

Confirmed: the category suppression predicate is shared by plot and list;
`update_gallery` refreshes the comparison rows; library details use kind/raw
text; automatic reference colours start at index 1. Focused verification:
`./.venv/bin/pytest tests/test_comparison_panel_model.py tests/test_add_reference_dialog.py tests/test_comparison_list_widget_rebuild.py tests/test_reference_series_palette.py -q`
→ **48 passed in 6.66s**. `py_compile` of add_reference_dialog,
cloud_reference_dialog, comparison_panel and main_window passed. These results
do not settle the live acceptance checks.

Blocking findings for a bounded corrective stage before 4c:
1. `ui/cloud_reference_dialog.py:1288–1359`: `set_taxon` changes the target,
   but `refresh` returns while an old search worker exists. Its completion
   accepts the old results unconditionally, without launching the new target's
   search. Reproduced offline with a real pane, a sentinel in-flight worker,
   and synthetic completion: current target `Amanita fulva`, accepted result
   `Cortinarius limonius`. Guard stale search/error and detail callbacks by
   request identity and ensure the latest requested target is actually loaded;
   `_on_detail_finished` at 1437 also accepts old detail unconditionally.
2. `ui/add_reference_dialog.py:425–428,488–498`: Return on the editable combo
   always parses display text, including an unchanged AI label. Calling the
   connected handlers with an 82% candidate reproducibly changes species from
   `rubellus` to `rubellus  82%`. Preserve structured selected-item data when
   text is unchanged; parse only deliberately edited free text. Tests currently
   call selection and typing handlers separately and miss this sequence.
3. `ui/main_window.py:11540–11545`: initial genus/species come from editable
   old reference-panel fields, while taxon ID comes from the active observation
   (`_active_sporely_taxon_id`, 13093). The old AI handler at 12948 changes those
   fields. After selecting another old-panel target, the dialog's “This
   observation” entry can name that target while Library filters by the actual
   observation ID. Initialize own-target identity consistently from the active
   observation; keep comparison target distinct. Add a mismatched-panel fixture.

Evidence/maintenance still owed: the manifest has 30 screens but the default
scenario forces 1400x760, the AI scenario is selected rather than open, and
suppressed/colours/default/AI lack dark counterparts. Preserve the earlier
static inspection report as a claim; this review did not re-inspect PNGs.
Required translation refresh was not committed: new selector/hint source
strings are absent from the Norwegian catalogue, and comparison_panel.py is
absent from tools/update_translations.sh's source list. Correct extraction
coverage and refresh the paired TS/QM files under the localization rules.

All seven manual checks above remain unconfirmed; “not run in this pass” is
not proof of an earlier pass. Extend acceptance to keyboard selection/Return,
rapid target changes with Community loading, and reopening after changing the
old panel target. Test geometry AND splitter persistence across app restart.
The completed prompt's split self-verifiable/post-commit gate conflicts with
AGENTS.md's human-gated interaction rule; the existing commit is not acceptance
and should not be rewritten. A corrective implementation must remain
uncommitted until its human gate passes. A focused stage-boundary correctness
review is warranted after correction; no authoritative security boundary change
was found that warrants a security-reviewer escalation.

## Subsystem map (verified 2026-09-03)

### Analysis-tab reference panel — `ui/main_window.py`
- `_build_reference_panel()` ~L9993 — builds the "Reference values" CollapsibleSection
  (`reference_section = CollapsibleSection(...)` L9357). Center of the feature.
- Plotted-dataset state (THE model):
  - `self.reference_series` (list[dict], init L7269) — the SET of datasets plotted + colors.
    Normalized entries; entries with `data["observation_reference_use_id"]` are the
    persisted library attachments.
  - `self.reference_values` (dict, init L7260) — legacy single active reference.
  - Row table: `self.ref_series_table` QTableWidget(0,5) L10124 — cols
    [Plot(checkbox) · color-chip("A") · Data set · Color · Library]; refreshed by
    `_refresh_reference_series_table()` L10766.
  - Ops: `_set_reference_series` L10936, add ~L10959, remove L10996,
    `_set_reference_series_enabled` L10720 (visibility toggle),
    `_set_reference_series_color` L10735 / `_open_reference_series_color_menu` L10609,
    `_reference_series_key` L10457.
  - Color palette: `reference_plot_palette()` L559 / `reference_plot_palette_groups()` L547 /
    `_blend_reference_palette_color()` L536. (Fixed ordered palette; obs = index 0 / blue.)
- Five entry points to remove/absorb (all in `_build_reference_panel`):
  1. Source dropdown: `self.ref_source_input` (editable QComboBox) L10035 +
     `_populate_reference_panel_sources()`; Plot: `self.ref_plot_btn` L10063 →
     `_on_reference_panel_plot_clicked`. → REPLACED by picker.
  2. Quick add: `self.ref_add_btn` "Quick add…" L10070 → `_on_reference_panel_add_clicked`;
     the "Add reference data" parse-string dialog is defined ~L5320-5365. → moves into
     picker "Enter manually" tab.
  3. Edit: `self.ref_edit_btn` L10079 → `_on_reference_panel_edit_clicked`. → dies (editing
     moves to per-row context menus).
  4. Community: `self.ref_cloud_btn` "Cloud..." L10039 → `_on_reference_panel_cloud_clicked`
     opens `CloudReferenceDialog`. → picker "Community" tab.
  5. Attach library: `self.ref_attach_library_btn` L10149 → `_on_attach_library_reference_clicked`
     opens `ReferenceLibraryAttachDialog`. → absorbed into picker "Library" tab (dialog dies).
- Manage library: `self.ref_manage_library_btn` L10157 → `_on_manage_reference_library_clicked`.
  → stays, becomes the small library icon-button next to "Add reference…".
- Taxon fields (panel copy): `ref_vernacular_input`/`ref_genus_input`/`ref_species_input`
  L10013-16. AI suggestions: `self.ref_ai_suggestions_combo` L10021 +
  `AISuggestionItemDelegate` + `_on_ref_ai_suggestion_activated` +
  `_refresh_reference_ai_suggestions()`. → taxon header chips (AI chips + "Edit taxon").
- Shape/MinMax: `self.ref_shape_selector` (SegmentedSelector) L10102 Ellipse/Square;
  `self.ref_show_minmax_checkbox` L10109. → move into Plot settings section
  (`plot_section = CollapsibleSection("Plot settings", ...)` L9356).
- Load/persist of series into per-obs settings blob: L22034-22078 (tab-switch/observation-open
  restore); `load_reference_values()` L23561.

### Community review pane — `ui/cloud_reference_dialog.py`
- `class CloudReferenceDialog(QDialog)` L129. Review pane is **inline** L263-322:
  `self.review_tabs = QTabWidget()` L263 with tabs Summary (L322), Raw spores (L295),
  Method (L314), Calibration (L318), Provenance (L322).
  - Summary tab: `summary_title_label`, `summary_meta_label`,
    `summary_table` QTableWidget(3,4) cols [Metric · Min · Median/Mean · Max] L277,
    `summary_note_label`.
  - Method tab: QFormLayout rows mount/stain/sample_type/contrast/objective/scale (`_method_labels`).
  - Raw spores / Calibration / Provenance: read-only QPlainTextEdit.
- Two footer buttons ("Import summary as reference" / "Use raw points for plot") → become a
  radio inside the Community tab ("Range summary" / "Raw points (n=X)").

### Shared tab style
- **No custom QTabWidget subclass.** Tabs are plain `QTabWidget` styled globally by QSS in
  `ui/styles.py:390-436` (`QTabWidget` / `QTabWidget::pane` / `QTabBar::tab` rules).
  → Picker source tabs AND the extracted preview sub-tabs must use plain `QTabWidget`.

### Other dialogs
- `ui/reference_library_manager_dialog.py` — Reference Library window (stays as curation
  back-office). Remove its "Attach to active observation" button + "Role: Compared" dropdown.
- `ui/reference_library_attach_dialog.py` — `ReferenceLibraryAttachDialog` (DIES; absorbed).
- Edit measurement set dialog — stays as-is (opened from library + per-row menu).
- `references/measurement_parser.py`, `references/reference_plotting.py` — backend, untouched.

### Persistence (open decision — RESOLVED: PERSISTED per-observation)
- `ObservationReferenceUseRepository` (imported `ui/main_window.py:165`) backs the
  `observation_reference_uses` table. Library attachments carry
  `data["observation_reference_use_id"]`, are synced (plan §12) and exposed via public
  observation API. Design explicitly separates "plotted temporarily" (session-only, no use_id)
  from "attached to observation" (persisted). Ref: plan doc L95-101, L204-210, L279-303, L482-604.
- Decision: KEEP existing persistence. Comparison row model gets `persisted` = bool(use_id);
  persisted rows show a small pin icon. Do NOT invent new persistence.

## reference_series entry schema (verified — `ui/main_window.py`)
Normalized entry = `{"key", "data": {...}, "label": str, "enabled": bool}`;
`_resolved_reference_series_entries()` adds `"color"`. Key methods:
- `_normalize_reference_series_entry` L10524; `_reference_series_key` L10457;
  `_format_reference_series_label` L10480 (single combined title line today).
- `data` keys: `source_kind` ∈ {`observation`,`points`,`reference`(default)} — the SOURCE-KIND
  discriminator; `observation_reference_use_id` (str; present ⇒ persisted Library attachment ⇒ pin);
  `genus`,`species`; range fields `length_min/length_max`, `core_min/core_max` (typical),
  `width_*`, `q_*`; `points` (list) + `points_label`/`source_label`/`source_type` for point sets;
  `source` (e.g. publication/mycena.no), `mount_medium`,`stain`; library rows also
  `short_label`,`name_as_published`,`locator_text`; `plot_color` (per-row override).
- Source-kind → badge: `observation`⇒"My obs"; `points`⇒"Community" if from cloud/mycena.no
  (source_type/source), else point set; `reference`+`observation_reference_use_id`⇒"Library";
  `reference` w/o use_id⇒"Manual". (Refine cloud vs manual by source string.)
- Range detail string: `_format_dimension_range(dim)` L5868 → `(min–)p05–p50–p95(–max)` per L/W/Q.
- Ops: visibility `_set_reference_series_enabled(key,bool)` L10720; color
  `_set_reference_series_color(key,color)` L10735 / `_open_reference_series_color_menu` L10609;
  remove `_remove_reference_series_key(key)` L10966; add `_add_reference_series_entry(data)` L10946.
- "This observation" row is NOT in reference_series — it's the active observation's own spores drawn
  as the blue histogram (`hist_color` #3498db light / #4a90d9 dark, L10557). Row 1 is synthetic;
  source n/date/spores from the active-observation measurements feeding that histogram
  (keyed by `self.active_observation_id`).

## Custom widgets to reuse
- `CollapsibleSection`, `SegmentedSelector`, `AISuggestionItemDelegate` (all `ui/main_window.py`).
- Plain `QTabWidget` + global QSS for all tabs.

## Renderer (Constraints TODO resolved)
`QT_QPA_PLATFORM=offscreen ./.venv/bin/python -m tools.render_review_screenshots --scenario <id> <outdir>`
Existing `reference.*` scenarios incl. a dark one: `python -m tools.render_review_screenshots --list`.
Add scenarios for new widgets (comparison panel, picker) incl. long-content cases (8+ rows,
long publication title elision, æøå), light + dark.

## Stages (report after each; do not proceed past failing render/tests)
1. Extract community review pane → reusable `ReferencePreviewPane` widget (no visible change).
   Renderer check on community dialog.
2. Comparison list widget + row model backed by `reference_series`; add `persisted`/pin.
   Swap into panel ALONGSIDE old controls. Renderer check.
3. Add-reference picker dialog shell: 4 plain-QTabWidget tabs + shared ReferencePreviewPane;
   wire Library tab first. Title "Add reference — {taxon}". Renderer check.
4. Wire Community / My observations / Enter manually tabs (relocate existing flows). Renderer each.
   Split in execution: **4a** My observations + row-anatomy fix (`a36ec63`),
   **4a-fix** three live-app defects (`e8a67f1`), **4b** Community tab,
   **4c** Enter manually tab.
5. Reference context/provenance and reported-versus-derived presentation
   (re-scoped; no verdict computation). Follow-up defects fixed in `33b62e0`.
6. Remove the duplicate legacy Analysis reference form/table and entry points;
   preserve applicable row actions in the comparison list, compact library
   management, and move Shape/Min–Max into Plot settings. Human-gated.
   New taxon-chip design, palette redesign and unrelated polish are deferred.
7. Report persistence findings (above) + deferred items.

## Known issues (tracked, not yet fixed)
- Meta-line truncation: stage-1 screenshots of the extracted `ReferencePreviewPane`
  Summary tab show the meta label (source/date line) truncating without an ellipsis
  or tooltip on long content — pre-existing behavior carried over from
  `CloudReferenceDialog`, not introduced by the extraction. Needs a follow-up fix
  (elide + tooltip, matching the row-title elision pattern in
  `ui/comparison_panel.py::_ComparisonRowWidget.resizeEvent`) in a later stage;
  not in scope for stage 3.
- Summary-table header truncation (found in stage 4c): `ReferencePreviewPane`'s
  Summary tab "Median / Mean" column header clips on both edges without an
  ellipsis once translated to a longer string ("Median / Gjennomsnitt" in
  Norwegian) — first exposed by the stage-4c Enter-manually Norwegian
  scenario, since no earlier nb-no scenario showed a populated
  `ReferencePreviewPane`. Same class of defect as the meta-line truncation
  above; shared picker-wide infrastructure, not introduced by stage 4c's
  editor relocation and not in scope for it. Needs the same follow-up
  (elide + tooltip) in a later polish stage.
- Misleading confirm-dialog wording for a deliberate cross-species
  comparison (found in stage 4c manual testing; confirmed in code by the
  fresh independent review): the confirm dialog in
  `_persist_normalized_reference_from_dialog` (`ui/main_window.py:11741`)
  asks "...If this is an accidental edit, click No" whenever the submitted
  genus/species differ from the observation's own taxon record. That
  wording was written for the legacy panel's editable identification
  fields, where a mismatch usually did mean an accidental edit or a
  synonym/historical name. Reached from the Enter-manually tab, the
  mismatch is routinely the deliberate, expected case — comparing the
  observation against a different possible species — so the wording reads
  as if something might be wrong when it is not. The underlying data
  behavior is correct: the reference persists under the typed target's own
  name, and the observation's own identification is never touched. Needs a
  bounded follow-up stage to distinguish "editing the observation's own
  identification" from "choosing an independent comparison target" in this
  guard's condition/wording before it fires.

## Stage 3 pre-check: rebuild feedback-loop / scroll-preservation audit
Verified 2026-09-04 against `ui/comparison_panel.py` before starting the picker
dialog:
- **Re-emit risk: none found.** `_ComparisonRowWidget.__init__` calls
  `self.checkbox.setChecked(row.visible)` (L139) *before*
  `self.checkbox.toggled.connect(...)` (L140), and `set_rows` (L253) always
  constructs a fresh `_ComparisonRowWidget` rather than mutating an existing one's
  checkbox — so there is no code path where a rebuild's own state restoration can
  emit `toggled`/`visibility_toggled`. Confirmed with a widget-level regression
  test (`tests/test_comparison_list_widget_rebuild.py::test_set_rows_does_not_reemit_visibility_toggled`).
- **Scroll position: preserved.** `set_rows` clears row widgets from
  `self._list_layout` but never replaces `self._container` or calls
  `self._scroll.setWidget(...)` again, so the `QScrollArea`'s vertical scrollbar
  value is untouched by a rebuild with equal-or-similar content height. Confirmed
  with `tests/test_comparison_list_widget_rebuild.py::test_set_rows_preserves_scroll_position`
  (9-row list, scroll to max, rebuild, assert value unchanged).
- **Focus/flicker: deferred.** `set_rows` deletes and recreates every row widget
  even when only one row's visibility changed, which will visibly flicker and drop
  focus/keyboard-navigation state on every toggle. Not fixed in this stage;
  candidate for a later polish pass (stage 6) that diffs rows instead of
  rebuilding wholesale.
No code changes were required in `ui/comparison_panel.py` itself; only the two
regression tests above were added to lock in the verified-safe behavior.

## Landed stages

Backfilled 2026-09-04 from the commits. Stage prompts live in
`~/Documents/Code/sporely/.sparring/prompts/`.

### Stage 1 — `272b20d` — extract `ReferencePreviewPane`
Pulled the tabbed Summary/Raw spores/Method/Calibration/Provenance review UI out
of `CloudReferenceDialog` into `ui/reference_preview_pane.py`.
`CloudReferenceDialog` forwards its old attribute names via properties, so
existing callers are unaffected and the change is invisible at runtime.
Verified: renderer scenarios `reference.community-preview(-dark)`.
Deferred: the Summary tab's meta-line truncates without ellipsis or tooltip —
pre-existing, carried over from the dialog (see Known issues).

### Stage 2 — `8290371` — `ComparisonListWidget`
Added `ui/comparison_panel.py` (`ComparisonRow` + `ComparisonListWidget`) as a
per-row presentation of the existing `reference_series` state, wired additively
below `ref_series_table`. Both lists refresh from the same
`_refresh_reference_series_table()` call site and route mutations through the
existing enable/colour ops, so no state was forked.
Verified: model-level tests for row creation/removal, visibility round-trip,
observation-row pinning; renderer scenarios `reference.comparison-list(-dark)`,
`-overflow`, `-longnames`.
Deferred: overflow menu edit/view/remove are stage-6 placeholders.

### Stage 3 part 0 — `bb65ed8` — rebuild-safety audit
Audited `set_rows` for the checkbox rebuild feedback loop and scroll
preservation before the picker work. Both properties already held
(`setChecked` before `connect`; the scroll area's content widget is reused, not
replaced), so no production change was needed.
Verified: `tests/test_comparison_list_widget_rebuild.py`.
Deferred: `set_rows` still recreates every row widget on any change — flicker
and focus loss are a stage-6 diffing pass.

### Stage 3 — `ab4bb38` — `AddReferenceDialog` shell + Library tab
Added the tabbed Library/Community/My observations/Enter manually picker. Only
Library wired: taxon + text filtering (AND semantics), preview pane populated
from the selected measurement set, "New publication…" reusing the existing
editor, and "Add to plot" routed through the existing
`_attach_normalized_reference_to_active_observation` so persistence and colour
assignment are not duplicated. Other three tabs show an honest placeholder. An
additive "Add reference…" button opens the picker; legacy entry points untouched.
Verified: `tests/test_add_reference_dialog.py`; renderer scenarios for populated,
empty-state, and stubbed tab.

### Stage 4a — `a36ec63` — row anatomy + My observations tab
Fixed the stage-3 row-anatomy gap (Library rows now render a second,
independently elided detail line). Wired the My observations tab over the same
query backing the legacy "My data &lt;date&gt;" entries, and hoisted
`ReferencePreviewPane` to dialog level so all tabs share one instance. A personal
observation has no measurement-set identity, so `MainWindow` dispatches on an
`observation:<id>` identifier prefix to the pre-existing legacy comparison-series
path instead of the normalized attach path.
Verified: `tests/test_add_reference_dialog.py`; renderer scenarios.
Deferred: Community → 4b, Enter manually → 4c.

### Stage 4a-fix — `e8a67f1` — three live-app defects
Three defects seen in a screenshot of the running app. (1)
`from_resolved_entry` now maps the legacy `source_kind == "observation"` string
to `SourceKind.MY_OBS`, leaving `SourceKind.OBSERVATION` reserved for the
current observation; this also removed the reserved-blue override that caused
the chip/plot colour mismatch. (2) Added
`ComparisonRow.for_current_observation` +
`MainWindow._current_observation_comparison_row` to synthesize pinned row 1 from
the active observation's own measurements, since
`_resolved_reference_series_entries` holds references only. (3) The
current-observation checkbox renders checked-and-disabled, as its scatter has no
independent toggle yet.
Verified: model tests + renderer scenarios updated to observation/library/my_obs;
all five manual tests confirmed passing by the user.

**Open defects carried into 4b** (from the 4a-fix review, 2026-09-04):
- `_current_observation_comparison_row`'s `n` counts a different population than
  the plot draws — it ignores the `gallery_filter_combo` category filter, omits
  the plot's `width > 0` check, and hand-rolls a category allow-list instead of
  reusing `normalize_measurement_category`. The category filter makes this
  user-reachable today.
- `test_chip_color_matches_plotted_color_after_a_reordering_sort` is
  tautological: it sorts a list of dataclasses and asserts their `color` fields
  are unchanged, which cannot fail. It gives no regression protection. Chip and
  plot colour cannot in fact diverge — both read `entry["color"]` from the same
  `_resolved_reference_series_entries` call — so the reserved-blue override was
  the whole cause, and the prompt's index-vs-sort-order hypothesis was a wrong
  diagnosis of a real symptom.
- `set_rows`' pin-first sort is now redundant with the caller's
  `rows.insert(0, current_row)`, since no reference row can carry
  `is_observation=True`. Two mechanisms for one job.

### Stage 4b-fix2 — `3d81ace` — target-state correction and acceptance evidence

Corrected obsolete Community responses, Return parsing of selected taxon labels,
and inconsistent own-target initialization; completed scoped translations and
natural-size/open-popup renderer coverage. Independent review confirmed the
required 62 tests, the two new capture tests, syntax, translations and 12 scoped
screenshots, and the user confirmed all seven manual checks passed on
2026-09-05. The expanded test run was 71 passed/3 failed solely on the documented
pre-existing renderer-inventory assertions; that debt is carried explicitly
into 4c's scenario-test maintenance. Committed and prompt archived; 4c is the
next bounded stage, not implemented here.

### Stage 4c — `de305fb` — Enter-manually tab wired

Replaced the "Enter manually" placeholder with a real, shared entry flow.
Extracted the legacy `ReferenceAddDialog`'s ~1500-line body into a new
`ui/reference_entry_editor.py::ReferenceEntryEditor`; `ReferenceAddDialog`
is now a thin modal wrapper delegating to an embedded instance via
`__getattr__`, so no parser/validation/normalized-payload logic is
duplicated. The picker's manual tab embeds the same editor without modal
chrome, sharing the dialog's `ReferencePreviewPane` and Add-to-plot/Cancel
footer. Both entry points now submit through one extracted routine,
`MainWindow._submit_reference_editor_result`.

Resolved the persistence-contract blocker the prompt required a fix for
before implementation could proceed: `ReferenceEntryEditor` now carries a
dedicated `observation_taxon_id` (the observation's own, fixed) alongside
`sporely_taxon_id` (the currently selected comparison target's, which may
legitimately differ). `_persist_normalized_reference_from_dialog`'s drift
guard reads the former; the treatment-creation branch still reads the
latter. Callers predating the picker's target selector never set the new
field, so their existing behavior is unchanged. `set_comparison_target`
resets publication/treatment identity and any built result on every target
change while preserving already-entered measurement values.

Verified by a fresh independent review (own session, after the user's
manual testing): reuse architecture, submission-path sharing, the
drift/failure gate (`_add_manual_callback` → `accept()` only on success;
`Cancel` never writes), and the taxon-identity separation were each
checked against the cited symbols/line ranges, not accepted from the
report alone. Required test/translation/screenshot commands rerun and
passed (58 + 64 + 12 tests; `py_compile`; `git diff --check`; zero
unfinished translations in all three languages); manual-tab screenshots
inspected directly. User confirmed all seven manual checks, including a
deliberate different-species comparison (added a *Mycena metata* reference
to an observation identified as *Mycena leptocephala*; the reference
persisted independently under its own name and the observation's
identification was untouched).

Deferred (see "Known issues"): the Summary-table "Median / Mean" header
clips in Norwegian (shared `ReferencePreviewPane` infrastructure, not
introduced here); and the observation-taxon-mismatch confirm dialog's
wording/guard assumes an accidental edit or synonym, which is misleading
when the mismatch is a deliberate cross-species comparison from the new
tab — data behavior is correct, only the dialog's condition/wording needs
a bounded follow-up.


### Stage 5 — re-scoped 2026-09-05: reference context/provenance, not verdicts

**Correction:** the approved plan's original stage 5 wording, "verdict
computation (pure, unit-tested) + badges + preview banner", is withdrawn.
Match/verdict/statistical-comparison work (whether an observation agrees
with a reference, any containment/overlap/threshold rule, any green/
yellow/red-style result) is **out of scope for this UI-unification plan
entirely** and does not belong here even in deferred/placeholder form. That
work's durable home is
`docs/plans/active/2026-07-12-parmasto-matching-foundation.md`, which this
same scoping pass expanded to explicitly own it (community-statistics
pipeline, cohort/filter selection, manual observation inclusion/exclusion,
transparent L/W/Q comparison, later green/yellow/red presentation,
eventual historical-reference-vs-community comparison — see that document;
exact thresholds and full Parmasto/Mahalanobis scoring remain deferred
there, not designed now).

Stage 5 in *this* plan is re-scoped to **reference context/provenance +
badges + preview summary**: registering what a published or community
source actually reported faithfully enough to support later evaluation,
without this stage judging whether it holds up. Motivation: published spore
ranges are often reused for decades without retaining how they were
obtained (method, medium, sample state, count, whether a later source made
new measurements or merely repeated an earlier one) — this stage is about
not losing that context, not about scoring it.

**`ComparisonRow.verdict` scaffold — reviewed and should be removed, not
reused.** `ui/comparison_panel.py`'s `verdict: object | None = None` field
(~L87) and the empty `verdict_label` `QLabel` (~L236) were added in an
earlier stage explicitly anticipating match-state ("verdict computation
lands in stage 5... empty while verdict is None"). That anticipated
semantics is now out of scope for this plan, so keeping the field/widget
around under that name would mislead a future reader into thinking a
match/no-match slot is still coming here. **Recommendation for the stage-5
implementer: remove `ComparisonRow.verdict` and `verdict_label` entirely**
(update `from_resolved_entry`/`for_current_observation` and the two existing
tests asserting `verdict is None`, `tests/test_comparison_panel_model.py`
~L109/169, accordingly) rather than repurposing or renaming them — this
stage's provenance context surfaces through the row's existing detail
line/tooltip and the picker's existing Summary/Method tabs, not through a
new verdict-shaped slot next to the source-kind badge.

**What already exists and needs no schema change (confirmed by reading the
code in this pass):**
- `reference_measurement_sets` already has `mount_medium`, `stain`,
  `preparation`, `measurement_method`, `sample_size`, `specimen_count`, and
  a free-text `notes` column (`database/reference_library_schema.py`
  ~L110-145); `reference_taxon_treatments` already has `name_as_published`,
  `locator_text`, `page_from/page_to`, and a free-text `treatment_notes`
  column (~L92-107).
- `ReferencePreviewPane`'s existing Method tab (`ui/reference_preview_pane.py`
  ~L79-100, populated at `ui/add_reference_dialog.py:737`,
  `ui/cloud_reference_dialog.py:1158/1497`, `ui/reference_entry_editor.py:659`)
  already surfaces `mount_medium`/`stain`/`preparation`/`measurement_method`
  for all four source tabs today — but under labels written for an
  observation's own microscopy setup ("Objective / profile:", "Sample
  type:"), not for a literature/community source's reported method. The
  labels need literature-appropriate wording, not new data plumbing.
  `measurement_set.notes` is already read but funnelled into
  `set_calibration(...)` (`ui/add_reference_dialog.py:745`), i.e. shown as
  "calibration details" rather than as general source notes — another
  existing mislabel, not a missing field.
- `reference_taxon_treatments.treatment_notes` is captured by the schema but
  **not read anywhere in the UI** today (confirmed: no match for
  `treatment_notes` outside the schema file). Threading it through to the
  picker (e.g. into the Summary tab's note area) is new plumbing, but reuses
  an existing column — no schema change.
- `name_as_published` already flows into a **Library-attached** row's title
  today (`MainWindow._format_reference_series_label`, `ui/main_window.py`
  ~L8977, the `observation_reference_use_id` branch, ~L8981-8988) but is
  captured and then silently dropped for **Manual-tab** entries: the editor
  already builds `"name_as_published": self.name_as_published_input.text()`
  into its payload (`ui/reference_entry_editor.py:1158`), but the row-label
  function's non-Library branch (~L8989 onward) only ever reconstructs a
  label from `genus`/`species`, never reads `name_as_published`. This is a
  concrete, already-identified bug relative to "preserve the taxon name as
  published, separately from current taxonomic mapping" — fixing it needs
  no new field, just reading one that already exists.
- The Q-value/typical-range "reported vs. Sporely-derived" distinction
  already exists as *behavior* (stage 4c's extreme→typical fallback in
  `ReferenceEntryEditor._refresh_preview`) but not as a *visible* distinction
  in the picker's Summary table or the comparison row — today a computed
  fallback value renders identically to a directly reported one.

**What is out of scope for Stage 5 because the schema does not capture it
yet, and adding it is a data-model decision, not a UI task** (do not invent
new columns for these in this stage): specimen state/age, voucher/source
material, and geographic origin have no existing field anywhere in
`reference_library_schema.py`. An explicit "original description vs.
secondary/repeated reference" *structured* flag likewise does not exist.
Where a cataloger has already typed something relevant into the existing
free-text `notes`/`treatment_notes` columns, Stage 5 may display it
verbatim as-is (no parsing/classification); it must not attempt to infer or
assert original-vs-secondary status from any other signal. Treat all of the
above as "not reported" when absent — never as a quality signal, and never
render an icon/color that implies judgment about the reference itself.

**Proposed smallest Stage 5 (engineering-only; no schema change, no new
persistence, no match/verdict logic):**
1. Fix `_format_reference_series_label`'s non-Library branch to prefer
   `data.get("name_as_published")` when present, falling back to the
   existing genus/species reconstruction only when it is absent — for
   every source kind, not only Library-attached rows.
2. Thread `treatment_notes` and the measurement set's `notes` through to the
   picker/comparison row as plain, labelled, non-judgmental context (e.g. a
   "Source notes" line/tooltip), keeping `notes` additionally available
   under its existing Calibration-tab meaning if that reuse is still
   accurate, or relabelling if it is not — implementer's call within
   existing widget conventions.
3. Re-word the existing Method tab's field labels
   (`ui/reference_preview_pane.py`'s `_method_labels`) so they read as
   *reported source method* ("Mounting medium (as reported):", "Preparation
   (as reported):", etc.) rather than *current observation's own microscopy
   setup*, and apply the existing "—" / "not reported" convention
   consistently rather than an empty string.
4. Add a compact, non-judgmental provenance line to the comparison row's
   existing detail/tooltip (`ui/comparison_panel.py::_format_detail` and/or
   `_ComparisonRowWidget`) surfacing whichever of method/medium/stain/
   sample size/specimen count are actually present, with "not reported" for
   the rest — reusing the row's existing elision/tooltip pattern, not a new
   widget mechanism.
5. Mark values in the picker's Summary table that came from the existing
   extreme→typical fallback as derived rather than reported (e.g. a
   footnote/tooltip on the affected cell), instead of rendering identically
   to a directly-reported value.
6. Remove `ComparisonRow.verdict`/`verdict_label` per the critique above.
7. No new preview "banner" widget is required if `summary_note_label`
   (already present, currently used for unrelated instructional copy) is
   repurposed to show a one-line provenance summary — implementer's call
   whether to repurpose it or add a second label, so long as the existing
   instructional copy it currently carries in the empty/no-selection state
   is preserved somewhere (it is still useful and unrelated to this
   change).

None of the above requires a new column, table, migration, or cloud-sync
change, and none of it computes or persists any match/agreement judgment.
**This is judged concrete enough for a bounded implementer stage; see
`.sparring/prompts/sporely-py/stage-5.md`.**

**Stage 5 — implemented, self-verified.** Files/symbols touched:

- `ui/comparison_panel.py`: removed `ComparisonRow.verdict` and
  `_ComparisonRowWidget.verdict_label` (dataclass field, both constructors,
  widget slot). Added `ComparisonRow.provenance` (default `""`) and a new
  `_format_provenance(data)` free function surfacing method/mount
  medium/stain/sample size/specimen count as a tooltip on
  `_ComparisonRowWidget.detail_label`, "not reported" for absent fields.
- `references/reference_plotting.py`: `_translate_range_or_summary` and
  `_translate_raw_points` now copy the snapshot's existing `method` sub-dict
  (`mount_medium`/`stain`/`preparation`/`measurement_method`) and
  `measurements.specimen_count` into the plotting `data` dict — this data was
  already captured by `build_snapshot` (`database/reference_citation.py`) at
  attach time but never threaded through to the comparison row.
- `ui/main_window.py::_format_reference_series_label` (~L8977): non-Library
  branch now prefers `data.get("name_as_published")` before the
  genus/species reconstruction, for every source kind.
- `ui/reference_preview_pane.py`: Method tab labels reworded to
  "as reported" phrasing (`_build_ui`). `set_summary` gained an optional
  `derived` parameter (parallel list of `(min,median,max)` bool flags) that
  appends a footnote marker + tooltip to fallback-derived cells. Added
  `provenance_summary_label` + `set_provenance_summary(text)` (a second
  label, not a repurpose — `summary_note_label`'s existing empty-state copy
  is untouched).
- `database/reference_library.py::MeasurementSetCandidate`: added
  `treatment_notes` (new plumbing reading the existing
  `reference_taxon_treatments.treatment_notes` column, no schema change);
  threaded through `list_attachment_candidates`'s query.
- `ui/add_reference_dialog.py::_populate_preview`: Provenance tab gained a
  "Source notes: {treatment_notes or Not reported}" line; kept
  `measurement_set.notes` → Calibration wiring as-is (implementer's call:
  still an accurate description of what that free-text column holds).
  Added a `set_provenance_summary` call (work/year, sample size, method
  recorded — never agreement). `_populate_observation_preview` (My
  observations tab) clears the summary line, since a personal observation
  isn't a published/community reference.
- `ui/reference_entry_editor.py::_refresh_preview`: `use_existing_radio`
  branch now fetches the paired `TaxonTreatment` for `treatment_notes` and
  fills the previously-empty `set_provenance("")` call; also sets a
  provenance summary line (looked up via `ReferenceWorkRepository`). The two
  manual-entry-in-progress branches (raw points, range-entry table) set
  `set_provenance_summary("")` — there is no external "reported by" source
  yet for data the user is actively typing in. The range-entry branch's
  `_bound`/`_mean` fallback helpers now return `(value, is_derived)` and
  `_refresh_preview` passes the per-cell flags to `set_summary(..., derived=...)`.
- `ui/cloud_reference_dialog.py::community_detail_preview_fields`: added a
  `provenance_summary` field (contributor/date/measurement count/whether any
  QC method flag was recorded); wired at both `set_provenance_summary` call
  sites. Community's `detail` dict has no `reference_taxon_treatments`/
  `reference_measurement_sets` columns at all (separate Supabase pipeline),
  so the "Source notes" treatment_notes/measurement_set.notes threading from
  Part 4 does not apply here — confirmed by reading
  `community_detail_preview_fields` before concluding this.

Reuse: `TaxonTreatmentRepository.get`/`ReferenceWorkRepository.get` (existing
CRUD, same pattern already used elsewhere in `main_window.py`); the row's
existing tooltip/elision pattern (no new widget mechanism); `set_summary`'s
existing `note`/`summary_note_label` split (added a second label rather than
overloading `note`, which already carries heterogeneous per-caller content).

Deviations: measurement_set.notes keeps its existing Calibration-tab meaning
rather than being relabeled — both are defensible per the prompt's
"implementer's call, documented either way" language; a future stage could
revisit if a cataloger reports `notes` is actually being used for citation
context rather than calibration.

Validation:
- `./.venv/bin/pytest tests/test_comparison_panel_model.py
  tests/test_comparison_list_widget_rebuild.py tests/test_add_reference_dialog.py
  tests/test_reference_entry_editor.py tests/test_reference_series_palette.py
  tests/test_reference_library_desktop_slice.py
  tests/test_reference_successor_adoption.py
  tests/test_community_results_pane_requests.py -q` → 144 passed.
- `./.venv/bin/python -m py_compile` on every touched file → clean.
- `./tools/update_translations.sh` (nb_NO/sv_SE/de_DE) → 0 unfinished after
  filling the 26 new strings via `tools/agent_translate.py`.
- Renderer screenshots (`--group reference-library`, light/dark/nb_NO) for
  new scenarios `reference.provenance-preview(-method)(-dark)(-nb-no)`, plus
  re-rendered `reference.comparison-list` and `reference.add-dialog-library`
  — inspected directly. Method tab shows reworded labels with the "—"
  not-reported convention in all three states; Summary table shows a `†`
  footnote marker + tooltip only on cells derived from the typical-range
  fallback, not on directly-reported cells; the new provenance-summary line
  renders above the Summary table. Screenshots are layout evidence only —
  the tooltip's actual runtime behavior is covered by
  `test_provenance_surfaces_reported_fields_and_not_reported_for_the_rest`
  and `test_summary_cell_derived_from_typical_range_marks_distinctly`, not
  by the screenshot itself.

No schema/persistence change; no match/verdict/threshold logic introduced.
Self-verifiable stage — committed as its own commit.

### Stage 5 independent review — 2026-09-06

Partly confirmed against `80dc1b858cd8c42856ebc686466b41350d7a328a`.
The verdict scaffold removal, published-name preference, existing snapshot
metadata plumbing, and preview presentation changes are present in the diff.
Acceptance is withheld pending two provenance-description fixes:

- `ui/cloud_reference_dialog.py:426` uses `bool(qc_lines)` for method presence.
  Direct calls reproduced geometry-only QC saying “method recorded: yes”,
  and an actual `mount_medium="KOH"` without QC saying “not reported”.
- `ui/reference_entry_editor.py:757` marks an entered species mean as derived;
  `_parmasto_value` at 1008 only parses the entered float. The shared tooltip
  incorrectly says this was derived from a typical range.

Focused reviewer verification: `QT_QPA_PLATFORM=offscreen ./.venv/bin/pytest
tests/test_reference_entry_editor.py tests/test_community_results_pane_requests.py
-q` — 20 passed. These existing tests do not cover the two defects. Static
screenshot claims were not independently revalidated in this review; no full
acceptance is implied. No implementation was changed by the reviewer.
Next scope is `.sparring/prompts/sporely-py/stage-5-fix.md`, pinned to the
reviewed commit; Stage 6 remains deferred. Standard implementation with focused
tests is sufficient; no authoritative security boundary is touched.

### Stage 5 follow-up — implemented, self-verified

Fixed both reviewed defects; no schema/persistence/matching/statistical
change and no Stage 6 control removal.

- `ui/cloud_reference_dialog.py::community_detail_preview_fields` (~L426):
  `method_recorded` no longer reads `bool(qc_lines)` (which counted
  `has_point_geometry`). It now checks whether any of the actual displayed
  method fields (`method_mapping["mount"|"stain"|"sample_type"|"contrast"|
  "objective"]`) is populated (`!= "—"`). Geometry-only QC now reads "not
  reported"; a reported `mount_medium` with no QC flags now reads "yes". The
  QC signals line/display itself (`qc_lines`, `note`) is untouched.
- `ui/reference_entry_editor.py::_refresh_preview::_mean` (~L753–758): the
  fallback to `_parmasto_value` (a directly entered "Species mean" field, not
  a typical-range derivation) now always returns `derived=False`. The numeric
  fallback value itself is unchanged; only the false derived marker/tooltip
  is removed. The extreme→typical `_bound` fallback (genuinely derived) is
  untouched.

Reuse: existing `method_mapping`/`_join_list` computation and `_parmasto_value`
parsing — no new helpers.

Validation:
- `QT_QPA_PLATFORM=offscreen ./.venv/bin/pytest
  tests/test_reference_entry_editor.py
  tests/test_community_results_pane_requests.py
  tests/test_comparison_panel_model.py
  tests/test_community_detail_preview_fields.py -q` → 36 passed. New file
  `tests/test_community_detail_preview_fields.py` adds four cases direct on
  `community_detail_preview_fields` (geometry-only QC, reported method with
  no QC flags, neither, both) reproducing and closing the reviewed defect;
  `tests/test_reference_entry_editor.py::test_reported_species_mean_is_not_marked_derived`
  drives the real `ReferenceEntryEditor` (extreme bounds + a Parmasto species
  mean, no min/max-table mean, no typical range) and asserts the mean cell
  has no tooltip; the existing
  `test_summary_cell_derived_from_typical_range_marks_distinctly` (typical-bound
  fallback) is retained unchanged.
- `./.venv/bin/python -m py_compile ui/reference_entry_editor.py
  ui/cloud_reference_dialog.py ui/reference_preview_pane.py` → clean.
- `git diff --check` → clean.
- No new/changed `tr(...)` strings — translations unaffected.
- Renderer: added `reference.add-dialog-manual-species-mean` scenario
  (`tools/review_ui/scenarios/references.py`), driving the real embedded
  `ReferenceEntryEditor` (extreme bounds + Parmasto species mean for Length;
  fully reported Width; typical-range-only Q) through `_refresh_preview`.
  Inspected the screenshot: Length mean "9.50" renders with no `†`/tooltip;
  Q min/max render with `†` from the genuine typical-range fallback — both
  states visible side by side from real widget state, not a hand-set fixture.
  Screenshot is layout evidence only; the no-tooltip assertion is covered by
  the unit test above, not the image.

Self-verifiable stage — committed as `33b62e0`. Requesting fresh independent
acceptance before Stage 6.

### Stage 5 follow-up independent acceptance — 2026-09-06

Confirmed the bounded fixes in `33b62e0`, reviewed at
`f1a461afd05fd9bf76cdbf6ccb465fb3162adcc3`. Community method presence now
uses the displayed mount/stain/preparation/contrast/objective values rather
than QC geometry. The entered species-mean fallback retains its value without
the false derived marker; typical-bound fallback markers remain intact.

Reviewer reran `QT_QPA_PLATFORM=offscreen ./.venv/bin/pytest tests/test_reference_entry_editor.py tests/test_community_results_pane_requests.py tests/test_comparison_panel_model.py tests/test_community_detail_preview_fields.py -q`:
36 passed. Inspected the manifest-listed image at
`/private/tmp/sporely-shot/reference.add-dialog-manual-species-mean.png`:
Length mean 9.50 has no dagger; Q bounds 1.20/1.40 have daggers, with the
relevant values and headers legible. Tooltip correctness is established by
the regression test, not the screenshot. No new translation strings or
schema/persistence/security-boundary changes in this follow-up.

The two defects from the previous review are closed. This acceptance covers
the bounded follow-up, not an expanded re-review of every Stage 5 surface.
No additional reviewer subagent or security reviewer is needed. Stage 6
implementation remains a separate prompt/task; no implementation changes
were made in this review.

### Stage 6 ready for implementation — 2026-09-06

User requested deletion of the old reference section shown in screenshots.
Prompt: `.sparring/prompts/sporely-py/stage-6.md`, pinned to
`f1a461afd05fd9bf76cdbf6ccb465fb3162adcc3`. Scope removes the duplicate
form/table while retaining the unified picker/comparison list, moving overlay
controls to Plot settings, and preserving row removal/editing/library actions.
The old table currently owns update/successor actions and gates comparison
refresh; these dependencies must be migrated before deleting it. Existing
attachment, settings and taxon-identity semantics remain authoritative.

Human-gated: implementer runs focused tests and full Analysis renderer
evidence, then leaves changes uncommitted for the prompt's numbered manual
checks. No Stage 6 implementation was performed while writing this prompt.

## Current stage / handoff — 2026-09-06 (stage 6 implementation)

**Human-gated, uncommitted.** All agent-verifiable checks pass; the seven
manual checks in `.sparring/prompts/sporely-py/stage-6.md` are pending the
user, so nothing is committed and the prompt stays unarchived.

This pass began with a background `sporely-implementer` agent already having
made substantial progress (construction removal, comparison-list action
wiring, renderer scenarios, translations); the user asked that it not run in
the background, so it was stopped mid-flight and this top-level session
inspected the partial diff, kept everything valid, and finished the
remaining work in place (per AGENTS.md's "investigate before discarding"
rule) rather than restarting.

**Files touched:**
- `ui/main_window.py` — `_build_reference_panel` (~L8486) rebuilt: removed
  the legacy genus/species/vernacular/source form, AI-suggestions combo,
  Quick add/Edit/Cloud/Attach-library buttons, and the `ref_series_table`
  QTableWidget from construction entirely (not just visibility); now builds
  only `ComparisonListWidget` + an action row (`Add reference…` +
  `Manage reference library…`). The plot-panel builder (~L7818) gained a
  relocated `ref_shape_selector`/`ref_show_minmax_checkbox` (same widget
  names, signals, `reference_shape`/`reference_minmax` settings keys, and
  `_sync_reference_overlay_controls_state` range/mean enablement as before),
  as two separate `QFormLayout` rows rather than one crowded `QHBoxLayout`
  row (see fix below). `_refresh_reference_series_table` (~L9159) now
  populates only the comparison list, carrying the old table's per-row
  tooltip (`_format_normalized_reference_row_tooltip`) onto
  `ComparisonRow.label_tooltip`. New adapters
  `_find_reference_series_entry`, `_reference_series_row_is_editable`,
  `_edit_reference_series_row`, `_on_comparison_library_update_requested`,
  `_on_comparison_library_successor_requested` replace the deleted table's
  per-row Edit/Update-from-library/Review-successor actions with row-key-
  driven equivalents reusing `ReferenceAddDialog`, `_update_reference_use_
  from_library`, and `_review_reference_successor` unchanged.
  `_on_reference_series_row_clicked` (the removed table's cell-click
  handler) was deleted — its only caller was the table's own `cellClicked`
  signal, gone with the widget. Fixed one bug found in review: an
  unconditional `self._maybe_set_ref_vernacular_from_taxon()` call inside
  `load_reference_values` (called on every observation switch) reached
  `self.ref_vernacular_input.text()` with no `hasattr` guard, unlike its
  sibling widget accesses — this crashed the app on opening any real
  observation with a genus/species set (not caught by the renderer, whose
  scenarios all use `active_observation_id = None`, short-circuiting before
  that call). Fixed with a one-line `hasattr` guard at the top of
  `_maybe_set_ref_vernacular_from_taxon` (~L10759).
- `ui/comparison_panel.py` — `ComparisonRow` gained `can_edit`,
  `library_update_available`, `library_successor_available`,
  `observation_reference_use_id`, `label_tooltip` fields computed in
  `from_resolved_entry`; `_ComparisonRowWidget._open_overflow_menu` wires
  Edit/Update-from-library/Review-successor/Remove using these fields
  instead of the stage-2/3 disabled TODO placeholders, and `remove_requested`
  is finally connected end-to-end (`ComparisonListWidget` forwards all three
  new signals).
- `tools/review_ui/scenarios/references.py` — six new registered scenarios
  (`reference.analysis-panel-{empty,populated,suppressed,longnames,dark,
  nb-no}`) that build the real Analysis-tab panel via an actual (init-
  stubbed) `MainWindow` and `create_gallery_panel()`, per
  `.claude/rules/ui-screenshots.md`'s "real production widgets" rule, rather
  than an isolated widget. Fixed one bug found in review: the scenario's
  window-construction helper originally patched `db_schema.get_database_
  path`/`get_reference_database_path` to a second, separate fixture
  directory via `context.enter_fixture` — which pushes onto one process-
  lifetime `ExitStack` never unwound between scenarios (`tools/review_ui/
  context.py::ReviewContext.enter_fixture`) — silently redirecting every
  reference-library scenario registered afterward (the whole
  `add-dialog-manual-*` group) to an empty database for the rest of the
  renderer run. Reproduced with the full default renderer
  (`python -m tools.render_review_screenshots <dir>`, no `--scenario`
  filter): `reference.add-dialog-manual-range` failed with "publication
  ...  is missing from the picker". Fixed by having the new scenarios reuse
  the already-patched shared `_fixture(context)` database instead of
  standing up a second one — the panel doesn't need its own isolated data,
  only a real, already-initialized database to construct `MainWindow`
  against.
- `tests/test_main_window_reference_panel_taxon_lookup.py` — adapted
  `test_reference_series_table_shows_row_controls_and_respects_row_height`
  → `test_comparison_list_shows_row_and_marks_it_editable`, and the two
  update/successor table tests → `test_comparison_list_offers_*`, to assert
  against `ComparisonRow` fields and emit the new `ComparisonListWidget`
  signals directly, instead of a `QTableWidget` this stage deleted. Behavior
  assertions (row content, update/successor dispatch to the same host
  methods) are preserved, not dropped.
- `tests/test_reference_panel_mainwindow_e2e.py` — new
  `test_load_reference_values_does_not_touch_removed_legacy_widgets`: builds
  a real `MainWindow` with none of the legacy `ref_*` widgets attached
  (asserts `not hasattr(...)` for each), an observation with a real
  genus/species in a real SQLite fixture, and calls `load_reference_values()`
  directly. Verified this test fails with the exact `AttributeError` above
  when the `_maybe_set_ref_vernacular_from_taxon` guard is reverted (checked
  by hand, not left in the suite), then passes with the fix — this is the
  "observation-switch guards" coverage the prompt's Verification section
  asked for.
- `tests/test_render_review_screenshots.py` — `REFERENCE_IDS` was missing
  five stage-5 `reference.provenance-preview*` scenarios (a pre-existing gap
  from stage 5, unrelated to this stage but caught by the same inventory
  test) plus this stage's six `analysis-panel-*` and
  `add-dialog-manual-species-mean`; added all eleven so
  `test_registry_has_unique_semantic_ids_and_expected_groups`,
  `test_group_and_explicit_scenario_selection`, and the full default-
  renderer integration test pass again.
- `i18n/Sporely_{nb_NO,sv_SE,de_DE}.{ts,qm}` — refreshed; 0 unfinished
  entries in all three languages. No new translation strings were
  introduced by this session's own edits (the shape-row split reused the
  same two existing `tr()` strings); the bulk of the diff is from the
  earlier background pass's relocation of `_build_reference_panel`.

**Existing code paths reused, by symbol name:** `_remove_reference_series_key`,
`_update_reference_use_from_library`, `_review_reference_successor`,
`_attach_normalized_reference_to_active_observation`,
`_resolved_reference_series_entries`, `_format_normalized_reference_row_
tooltip`, `_reference_overlays_allowed_for_category`,
`_current_observation_comparison_row`, `ReferenceAddDialog`,
`_persist_normalized_reference_from_dialog`, `_clean_ref_genus_text`/
`_clean_ref_species_text`, `_active_sporely_taxon_id`,
`_sync_reference_overlay_controls_state`, `on_reference_overlay_setting_
changed`. No parallel implementation of any of these was created.

**Layout fix (found and fixed in this session's review, not present in the
required command output):** the relocated Shape/Min-Max row rendered as one
`QHBoxLayout` (`"Reference shape:"` label + segmented selector + `Min/Max`
checkbox) overlapped illegibly at the ~300px panel width the renderer's
`reference.analysis-panel-*` scenarios use (a width the old, wider legacy
form's shorter `"Shape:"` label had never been tested against). Confirmed
visually in the rendered PNGs before the fix (label/button/checkbox text
interleaved) and after (two separate `QFormLayout` rows, both clean in
light/dark/Norwegian at the same width). The longer label is intentional,
not reducible to `"Shape:"`: this panel's `"Plot:"` row above it already has
its own `Ellipse`/`Kernel density`/`Mean` selector, so an unqualified
`"Shape:"` would be ambiguous next to it.

**Validation run:**
```
QT_QPA_PLATFORM=offscreen ./.venv/bin/pytest tests/test_comparison_panel_model.py tests/test_comparison_list_widget_rebuild.py tests/test_reference_panel_mainwindow_e2e.py tests/test_reference_panel_taxon_drift_and_retry.py tests/test_reference_attach_persistence_e2e.py tests/test_reference_successor_adoption.py tests/test_reference_series_palette.py tests/test_add_reference_dialog.py tests/test_reference_entry_editor.py tests/test_main_window_reference_panel_taxon_lookup.py tests/test_render_review_screenshots.py -q
```
→ **133 passed** (required nine files: 110 passed; plus the taxon-lookup
file's 10 adapted tests and the full renderer test module's 12, including
the previously-crashing full-default-renderer integration test, now fixed).
An additional, broader regression sweep (not required, run for extra
confidence given the size of this stage) —
`test_reference_panel_coordinator_existing_set.py`,
`test_calibration_reference_recovery_ui.py`,
`test_legacy_reference_interactive_migration.py`,
`test_reference_add_dialog_focus.py`, `test_reference_add_dialog_
normalized.py`, `test_reference_quick_add_service.py`,
`test_reference_work_editor_human_form.py`,
`test_reference_values_taxon_lookup_integration.py` — **90 passed**.
`py_compile` of `ui/main_window.py`, `ui/comparison_panel.py`, `tools/
review_ui/scenarios/references.py`: clean. `python -m tools.
render_review_screenshots --list`: clean. `git diff --check`: clean.
`./tools/update_translations.sh` was already clean (0 unfinished per
language) from the earlier background pass; not re-run since this session's
own edits added no new `tr()` strings.

**Screenshot evidence** (rendered via `tools.render_review_screenshots
--scenario ...`, manifests under this session's scratch/tmp directories —
not claimed as a committed artifact):
- `reference.analysis-panel-empty`: no legacy form/table/Attach-library
  button anywhere in the panel; only `Add reference…` and `Manage reference
  library…` below an empty comparison list; Plot settings shows the
  relocated `Reference shape: Ellipse | Square` and `Min/Max` on their own
  rows, no clipping.
- `reference.analysis-panel-populated`: three comparison rows (Library/
  Manual/My observations badges) with distinct colour chips and an
  overflow `…` menu per row, at the same 440px constrained width the stage
  prompt asked for.
- `reference.analysis-panel-suppressed`: switching the category away from
  Spores dims all three reference rows and shows the "Reference data
  applies to spore measurements" hint, with no legacy-table guard
  involved (the removed `hasattr(self, "ref_series_table")` checks are
  gone from `_refresh_reference_series_table`).
- `reference.analysis-panel-longnames`: a long publication title and a long
  æøå community label elide correctly at this width.
- `reference.analysis-panel-dark`: same populated state in the real dark
  palette — comparison-list rows, action buttons, and the relocated
  Shape/Min-Max row all readable.
- `reference.analysis-panel-nb-no`: real Norwegian translator resolves
  "Referanseform:" (Reference shape), "Kvadrat" (Square), "Min/Max",
  "Legg til referanse…" (Add reference…), "Referanseverdier" (Reference
  values) with no clipping.
- `reference.add-dialog-manual-range`: rendered clean after the shared-
  fixture fix, confirming the picker's publication list is intact once the
  analysis-panel scenarios no longer redirect `db_schema` for the rest of
  the run.

**Deviations from the prompt:** none identified beyond the two defects
found and fixed during this session's own review (the crowded Shape/Min-Max
row, and the `_maybe_set_ref_vernacular_from_taxon` crash) — both are
corrections needed to actually satisfy the prompt's existing requirements
("controls remain usable", "audit old taxon/source widget accesses during
... observation changes"), not scope additions. New taxon-chip design,
palette redesign, statistical matching, and Stage 7 reporting remain
deferred, matching the prompt's explicit boundary.

**Verification tier: human-gated, uncommitted.** Every agent-verifiable
check above passes. Do not commit until the user runs the six numbered
manual checks in `.sparring/prompts/sporely-py/stage-6.md`:
1. Open an observation → Analysis: confirm the old form/table/Attach-
   library button are gone and the comparison list + Add reference are
   reachable at a resized window without blank gaps.
2. Open Add reference; exercise Library, Community, My observations, Enter
   manually; add one reference from each; confirm a single row and plot
   appear; try a different comparison taxon and confirm the observation's
   own identity is unchanged.
3. Toggle/recolor/remove rows and edit an applicable legacy row; confirm
   only the chosen reference changes; the current-observation row stays
   first and is not removable/recolorable; a library use with an available
   update/successor keeps that action reachable.
4. Open library management beside Add reference; confirm it still opens.
   Change Ellipse/Square and Min-Max in Plot settings; verify overlays
   update and range/mean enablement is correct.
5. Switch to a non-spore category and back: references suppress/restore
   without losing chosen visibility/colors. Switch observations, return,
   and restart the app: confirm attachments/settings restore with no
   errors from removed controls.
6. Switch observations while a picker is open and attempt to add: confirm
   it does not attach to the wrong observation. Check a many-row list for
   toggle flicker, focus loss, or unwanted scroll jumps.

Acceptance requires a fresh, independent `sporely-sparring` review in a new
session after these checks pass — this session does not accept its own
work.

### Stage 6 manual-test-2 fix pass — 2026-09-06

Manual test 2 failed on two specific cases; the other five checks passed. This
pass continued the same uncommitted Stage 6 diff above (did not reset or
reimplement) and fixed only those two defects.

**A — Community offered the current observation as its own comparison
reference.** `ui/cloud_reference_dialog.py::CommunityResultsPane.__init__`
gained `exclude_observation_cloud_id` (the active observation's own
`observations.cloud_id`, distinct from the local-sqlite `exclude_observation_id`
the My-observations tab already uses) and a new `_exclude_self_reference`
helper applied in both `refresh()`'s injected-results branch and
`_on_search_finished`. It drops only rows with `_kind == "observation"` whose
`observation_id` (the cloud `public.observations.id` returned by
`search_community_spore_datasets` — verified against
`sporely-web/supabase/migrations/20260803120000_lock_down_observation_sync_tables.sql:608-636`)
matches the excluded id — identity, never taxon/name/date/contributor text.
Published-reference rows (`_kind == "reference"`) have no observation
identity and are never filtered. `ui/add_reference_dialog.py`'s
`AddReferenceDialog.__init__` gained the matching `exclude_observation_cloud_id`
parameter, forwarded to `CommunityResultsPane`; `ui/main_window.py`'s
`_on_add_reference_clicked` passes `obs_for_own_target.get("cloud_id")`
(the same observation snapshot already used for genus/species/taxon there).
Tests added in `tests/test_add_reference_dialog.py`:
`test_community_excludes_exact_current_observation_by_cloud_id`,
`test_community_keeps_other_observation_of_same_taxon`,
`test_community_unrelated_taxon_rows_unaffected_by_exclusion`,
`test_community_no_exclusion_id_keeps_all_rows`.

**B — Library preview lost Length/Width while Q stayed populated.** Root
cause found in `ui/add_reference_dialog.py::AddReferenceDialog._populate_preview`:
a source reported only as a typical/unparenthesised range (the common case,
e.g. "7–12 × 4–6, q=1.5–1.8") is stored with `length_min`/`length_max`/
`width_min`/`width_max` left `None` and the typical bound in
`length_core_min`/`length_core_max`/`width_core_min`/`width_core_max`
instead (`database/reference_library.py`'s `MeasurementSet` dataclass has no
such `q_core_min`/`q_core_max` fields — Q's own extreme-or-typical fallback
already happens at write time in
`ui/reference_entry_editor.py::normalized_measurement_set_payload`, lines
~1586–1594). `_populate_preview` read only the extreme `*_min`/`*_max`
fields with no core fallback, so Length/Width showed dashes while Q (whose
fallback already happened at write time) showed values — exactly the
reported Funga Nordica/mycena.no symptom. The list row was never wrong: it
renders `candidate.raw_text` verbatim, not structured fields, so it was never
this bug's evidence and no list-string parsing was used to fix it. Fixed by
adding the same extreme-then-core fallback `_translate_range_or_summary` and
`range_payload_is_plottable` already use elsewhere in
`references/reference_plotting.py`, and marking core-derived cells with the
existing `derived=` "†"/tooltip convention `ReferencePreviewPane.set_summary`
already supports (used by `ReferenceEntryEditor`'s own live preview) instead
of silently substituting values. Tests added:
`test_preview_summary_falls_back_to_core_bounds_when_extremes_missing`,
`test_preview_summary_extreme_bounds_win_over_core_when_both_present`.

**Validation:** `QT_QPA_PLATFORM=offscreen ./.venv/bin/pytest
tests/test_comparison_panel_model.py tests/test_comparison_list_widget_rebuild.py
tests/test_reference_panel_mainwindow_e2e.py
tests/test_reference_panel_taxon_drift_and_retry.py
tests/test_reference_attach_persistence_e2e.py
tests/test_reference_successor_adoption.py tests/test_reference_series_palette.py
tests/test_add_reference_dialog.py tests/test_reference_entry_editor.py
tests/test_main_window_reference_panel_taxon_lookup.py
tests/test_render_review_screenshots.py -q` → **139 passed** (53 of those in
`test_add_reference_dialog.py`, up from 45 before this pass' 8 new tests).
`py_compile` of `ui/main_window.py`, `ui/comparison_panel.py`,
`ui/add_reference_dialog.py`, `ui/cloud_reference_dialog.py`: clean.
Re-rendered `reference.add-dialog-library{,-dark}`,
`reference.add-dialog-community{,-dark,-points,-empty}`,
`reference.add-dialog-manual-range`, `reference.add-dialog-manual-species-mean`,
`reference.analysis-panel-populated`, `reference.provenance-preview`: all
clean, no regressions. (`reference.add-dialog-library`'s Summary still shows
all-dash Length/Width/Q — confirmed pre-existing: that scenario's
`MeasurementSetCandidate` ids are never inserted as real rows into the
renderer's fixture reference-library DB, so `MeasurementSetRepository.get`
returns a set with no numeric fields at all, core or extreme; unrelated to
this fix and not part of either reported defect.)

**Verification tier: still human-gated, uncommitted.** No new commit; the
implementation remains the same uncommitted working-tree diff as before this
pass, now including the two fixes above. `.sparring/prompts/sporely-py/stage-6.md`
stays pending, not archived.

**What still needs re-checking:** only manual test 2 (Add reference →
Community with an already-synced Amanita muscaria observation: confirm it no
longer appears as its own candidate, another Amanita muscaria observation
still does, and a Library selection with only a typical/no-extreme range
shows real Length/Width numbers in Summary, not dashes). Manual tests 1, 3,
4, 5, 6 already passed against the same Add-reference dialog and comparison
list this pass did not change, and are not expected to be affected — rerun
only if something in this pass' diff plausibly touches them.

Acceptance still requires a fresh, independent `sporely-sparring` review in a
new session after manual test 2 is reconfirmed.

### Stage 6 — independently accepted — 2026-09-07

Fresh-session `sporely-sparring` review, independent of the implementation
session above. Verified against code, not accepted on the report alone:
`_exclude_self_reference` (`ui/cloud_reference_dialog.py:1336-1355`) and its
wiring through `AddReferenceDialog`/`main_window.py:9936` (defect A), and the
`*_core_min`/`*_core_max` fallback with `derived=` marking in
`AddReferenceDialog._populate_preview` (`ui/add_reference_dialog.py:703-754`,
defect B) match the report exactly. Independently reran the full required
test list (139 passed), `py_compile` on the four touched UI modules, and
`git diff --check` — all clean. Spot-checked the broader Stage 6 removal
claim: no `ref_source_input`/`ref_genus_input`/`ref_series_table`
construction sites remain in `main_window.py`; the legacy form is gone from
construction, and the `_maybe_set_ref_vernacular_from_taxon` hasattr guard is
present at its reported line. The user reports all 6 manual tests in
`.sparring/prompts/sporely-py/stage-6.md` pass.

`AGENTS.md` and `docs/plans/active/2026-07-12-parmasto-matching-foundation.md`
carry unrelated pre-existing uncommitted changes (routing-doc and a different
plan's edits) that predate Stage 6's `expected_starting_head` and are not part
of this stage; left uncommitted, untouched.

Commit: `86ff879af804543c29dc40a91b54075e39ec707d`. Prompt archived to
`.sparring/prompts/sporely-py/completed/stage-6.md`. Stage 6 is closed.

Manual-evidence clarification — 2026-09-07: the user reports checks 1–6
PASS, including exclusion of the current synced observation from Community
and correct Library typical-range Length/Width summaries. The test with
another suitable same-taxon observation is **UNAVAILABLE**, because only one
suitable Amanita muscaria observation exists. That case is not a verified
pass. A subsequent review invocation found Stage 6 already committed as
`86ff879` with its prompt archived and HEAD at `4597fb6`; it did not perform
a new independent acceptance review or create a duplicate Stage 6 commit.

### Landing note — UI transplant onto current main — 2026-09-17

The reference-discovery and cloud-account UX fixes were rebuilt as a narrow
branch from `origin/main` (`0ec2619`), carrying only the seven UI commits and
none of the cloud-sync / reported-statistics ancestry the original branch had
accumulated beneath them.

Two deliberate deviations from those seven commits, both caused by main having
moved underneath them:

- **Failure propagation from the manual Add-reference callback is not part of
  this transplant.** One transplanted test,
  `test_manual_callback_propagates_failed_submission`, asserted that the
  picker's `manual_attach_callback` returns `False` when
  `_submit_reference_editor_result` fails. That depends on the `-> bool` return
  contract introduced by `4efdf7d`, which belongs to the separately preserved
  `feature/reference-save-and-plot` work and was intentionally left out here.
  On main the helper is still `-> None` and `_add_manual_callback` returns
  `True` unconditionally. The test was removed rather than the production
  contract changed; propagation lands with that feature, not with this UI
  cleanup.
- **`_update_footer_state` keeps main's shape.** The "Only this taxon" commit
  also rewrote a block managing `save_to_library_btn` / `_saved_manual_set_id`
  and a duplicate library status hint. Main has none of those attributes — the
  save-to-library-from-picker flow lives on the same preserved branch — so that
  hunk was dropped. The commit's actual subject, the `_empty_library_hint()`
  extraction in `_populate_results_list`, applied unchanged, and the duplicate
  wording it also fixed does not exist on main.
