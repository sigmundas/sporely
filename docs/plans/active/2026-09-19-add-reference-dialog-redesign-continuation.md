# Add reference dialog redesign — continuation

Status: active implementation.
Branch: `feature/add-reference-dialog-redesign`.

Canonical design contract:
`docs/design/DESIGN-CONTRACT-add-reference-dialog.md`

## Continuation baseline

This is a continuation of
`docs/plans/active/2026-09-17-add-reference-dialog-redesign.md`. That plan's
Preflight, Stage 1 and Stage 2 are complete; this plan carries forward only its
Stages 3–7, renumbered contiguously as Stages 1–5.

- Original Stage 1 (scientific display semantics and chooser projection) was
  completed in the previous run.
- Original Stage 2 (Library row anatomy, relevance grouping and multi-select)
  was implemented and reviewed at commit `5e0d530`.
- `5e0d530` is the accepted working baseline for this continuation.
- The previous managed run is intentionally not being resumed because its
  finalization cycle repeatedly excluded tracked `.sparring/project.toml` from
  the reviewed candidate commit. Its `.sparring` artifacts are kept as
  historical evidence and must not be edited, reset or deleted.
- This continuation starts from current branch HEAD after the Git-policy commit
  and does not require redoing the original Stages 1–2.

### What the baseline already provides

Stages below refer to these existing outputs rather than to old stage numbers:

- **The display-semantics projection** — `references/reference_display.py`,
  covered by `tests/test_reference_display.py`. One pure, non-Qt interpretation
  of stored reference content: source kind, real-raw-points availability, outer
  and core ranges per metric with their explicit meaning, explicit percentile
  bounds, explicit mean/median/mean interval, sample size with its own
  `sample_size_origin`, reported-versus-derived flags, and the compact data
  labels `Raw data`, `Published range`, `5–95% range` and other explicit
  percentile labels. Later UI must ask this projection what a source contains
  instead of reinterpreting database fields in a widget.
- **The redesigned Library list** — `ui/add_reference_dialog.py` and
  `ui/library_source_row.py`, covered by
  `tests/test_add_reference_library_list.py`. Relevance grouping
  (`This taxon` → `Same genus` → `Rest of library`) with counts, the row
  anatomy and badges, independent `preview_selection` versus `checked_sources`
  state, multi-select and the `Add N to plot` footer.
- **Review scenarios** — `tools/review_ui/scenarios/references.py` already
  carries Library grouped/multi-select scenarios.

## Execution discipline

Every implementation stage must:

1. start from a working dialog and leave a working dialog;
2. be independently reviewable and mergeable;
3. state its own automated verification and human/UI verification;
4. name which PR #6 regression tests it reruns;
5. avoid carrying unfinished behavior that only becomes valid after a later
   stage;
6. stop and report if a required behavior depends on a separate unlanded
   feature rather than silently recreating that feature.

Production code should not be changed merely to make the mockup easier to
paint. Scientific semantics come first.

## Carried-forward findings

### Existing dialog seams to preserve

`ui/add_reference_dialog.py::AddReferenceDialog` owns one dialog with the four
required source tabs and a shared `ReferencePreviewPane` in a horizontal
splitter. Build on that structure; do not create a parallel dialog.

The preview is isolated in `ui/reference_preview_pane.py`. It currently uses a
3x4 summary table plus Raw spores / Method / Calibration / Provenance tabs. The
visual comparison can therefore be replaced inside the existing shared preview
component instead of duplicating preview logic in each source tab.

The manual tab embeds `ReferenceEntryEditor` inside a scroll area and reuses the
existing parser and normalized submission path. Preserve that reuse.

### No new schema migration is currently indicated

The stored `MeasurementSet` already has outer Length/Width/Q pairs, inner/core
pairs, scalar means, raw points, `measurement_details_json`, and
sample/method/provenance fields. The frozen measurement-content contract already
represents explicit range meaning: a metric `core_range` can be tagged as
`percentile_interval` with explicit `percentile_bounds`, while ordinary
reported/typical ranges remain distinct.

`Published range` versus `5–95% range` is therefore a projection of existing
stored semantics, not a new database enum or migration. The baseline projection
audit covered Library, Community, My observations and manual/parser output. If a
stage below finds a path that cannot carry the semantics without loss, stop and
report the exact boundary before changing schema.

### Separate historical dependency: Save to library

Current `main` deliberately does not contain the separate manual
`Save to library` picker flow from the preserved
`feature/reference-save-and-plot` work (`4efdf7d` historical candidate). That
work also contains failure-propagation behavior deliberately excluded from
PR #6.

The redesign must **not** silently re-create fragments of that historical
feature just because the mockup contains a `Save to library` button.

Before Stage 4 below, either:

1. land the save/plot feature separately onto current `main` and rebase this
   branch; or
2. explicitly authorize a new bounded implementation of the same behavior.

Until then, visual/manual-entry work may proceed without claiming N28 complete.

### PR #6 regression anchors

At minimum keep these behaviors under automated or explicit manual coverage:

- `tests/test_community_search_input.py` — genus-only/live Community search;
- `tests/test_my_observations_genus_browsing.py` — genus-level personal browsing;
- `tests/test_library_taxon_scope_hint.py` — Only-this-taxon empty-state logic
  while that control still exists;
- `tests/test_add_reference_dialog.py` — resizing/splitter/manual-scroll
  behavior;
- `tests/test_add_reference_dialog.py::
  test_repeated_new_publication_selection_opens_exactly_one_editor` — deferred
  publication editor / single-Cancel regression.

If the redesign intentionally removes or supersedes an old UI behavior, replace
its old regression test with a new test that pins the new contract rather than
simply deleting coverage.

---

# Canonical stage sequence

## Stage 1 — Comparison-first Summary and honest Raw spores states

**Goal:** replace the old summary table with the approved comparison
visualization, using the baseline display-semantics projection rather than
widget-local inference.

### Observation baseline

Create one baseline model for the current observation from its actual
measurements. Prefer injection from the host or another testable seam over
hidden duplicate database queries.

With no source selected:
- show the observation baseline as Length / Width / Q chips;
- show explanatory copy;
- do not show a table of dashes.

### Fixed domains

Create one domain object per dialog session for Length, Width and Q.

- derive it once from sensible metric bounds plus the observation and
  currently-known candidate content;
- freeze it for the session;
- never rescale merely because the selected source changes;
- if later asynchronous content falls outside the frozen domain, indicate
  overflow/clipping explicitly rather than silently changing scale.

Do not hard-code a source-specific domain.

### Metric rendering

For each metric:
- source = filled band;
- user's observation = outlined band;
- explicit 5–95 inner range remains visually distinct from outer min/max;
- explicit mean/median may have a mark/label;
- no centre mark for a published range that reports no centre;
- comparison delta is shown only for equivalent statistics.

For raw-data-derived summaries, document exactly which statistic the band and
centre represent. Do not mix min/max, 5–95 and median without labels.

### Raw spores tab

- actual raw source points: show actual rows/data;
- no raw points: show an honest explanatory state;
- never synthesize fake per-spore rows from a range.

Keep Method / Calibration / Provenance tabs functional and source-neutral.

### Tests

Pure rendering/view-model tests should cover:
- no selection baseline;
- raw data;
- published range without centre;
- explicit 5–95 plus outer range;
- explicit mean;
- explicit median;
- no like-for-like statistic => no delta;
- fixed domains across source changes;
- honest Raw spores empty state.

### Verification

Run:
- `tests/test_reference_display.py`;
- preview-pane tests;
- `tests/test_reference_preview_pane_placeholders.py`;
- relevant Add reference tests;
- `py_compile`;
- `git diff --check`.

### Human gate

Manually compare at least:
1. one raw-data source;
2. one ordinary published range;
3. one explicit 5–95 source;
4. one source with no mean/median;
5. no source selected.

Switch repeatedly between them and verify the axes do not jump.

### Exit criterion

The right pane communicates an honest comparison without requiring the user to
decode a numeric table.

---

## Stage 2 — Community and My observations on the new preview contract

**Goal:** adapt the two non-Library source tabs to the Stage 1 comparison model
without broadening scope unnecessarily.

### Work

- Community range result -> `Published range` unless the payload explicitly
  carries stronger semantics.
- Community raw-points result -> `Raw data`.
- My observations -> actual personal raw points.
- Keep shared preview source-neutral.
- Preserve live/genus Community search.
- Preserve genus-only My observations browsing.
- Do not invent `5–95%` semantics for cloud/community payloads that do not
  explicitly provide them.
- Do not create cross-tab multi-select behavior unless separately approved.

### Tests

Run and extend:
- `tests/test_community_search_input.py`;
- `tests/test_my_observations_genus_browsing.py`;
- `tests/test_reference_preview_pane_placeholders.py`;
- Community/My-observation preview tests.

### Human gate

- genus-only Community search;
- full species narrowing;
- genus-only personal observation browsing;
- source switch between Library/Community/My observations without stale
  preview content.

### Exit criterion

All non-manual source tabs feed one honest comparison model.

---

## Stage 3 — Manual-entry layout and semantic preview

**Goal:** adopt the approved manual-entry layout while continuing to reuse the
existing parser, validation and normalized payload logic.

This stage does **not** independently implement the separate historical
Save-to-library feature.

### Work

- one-column manual editor;
- publication search first;
- name-as-published and locator;
- real no-taxon warning;
- paste-and-parse is the primary measurement path;
- 3 x 5 plain inputs:
  `extreme min | typical min | mean | typical max | extreme max`;
- explicit keyboard tab order left-to-right per row;
- parse fills the inputs;
- edited inputs update the same `MeasurementContent` working state used by the
  parser;
- manual preview uses the Stage 1 comparison model live;
- preserve exact parser descriptors:
  - typical range remains typical;
  - explicit percentile interval remains percentile;
  - 5–95 is displayed as 5–95 only when explicit;
  - no midpoint-as-mean/median.

If the parser cannot round-trip a required descriptor through
`normalized_measurement_set_payload`, stop and fix that semantic boundary
rather than storing a visually correct but semantically degraded row.

### Tests

- parse published range;
- parse explicit 5–95 data;
- parse outer + core;
- manual edits preserve descriptors where applicable;
- swap L/W moves numbers and descriptors together;
- tab order;
- live preview;
- no invented centre;
- existing deferred publication-editor regression.

### Verification

Run:
- `tests/test_reference_entry_editor.py`;
- `tests/test_reference_editor_reported_statistics.py`;
- relevant Add reference tests;
- parser/measurement-content contract tests;
- `py_compile`;
- `git diff --check`.

### Human gate

Paste real examples for:
- ordinary `(extreme–)typical–typical(–extreme)` notation;
- a bare published range;
- explicit 5–95 statistics;
- a Parmasto-style reference.

### Exit criterion

Manual entry matches the approved information hierarchy and preserves
scientific semantics end-to-end.

---

## Stage 4 — Manual Save to library footer integration

**Blocked until the separate save/plot behavior is deliberately landed or
re-authorized.** See *Separate historical dependency: Save to library* above.

### Goal

Complete N28 without importing unrelated branch history.

### Work after dependency resolution

- `Save to library` visible only on manual tab;
- hidden on other tabs;
- saving does not imply adding to plot;
- adding after a successful save attaches the same saved measurement set rather
  than creating a duplicate;
- failure leaves the dialog open and reports failure honestly;
- footer copy describes the action rather than why a control is disabled.

Reuse the landed persistence path. Do not create a second implementation in the
redesign branch.

### Exit criterion

The final footer matches the mockup and the persistence semantics are backed by
one canonical save/attach path.

---

## Stage 5 — Localization, screenshot parity and acceptance

**Goal:** finish the visual contract without hiding semantic or regression debt.

### Work

- run repository localization workflow;
- translate all new/changed strings in nb_NO, sv_SE and de_DE;
- add/update deterministic review scenarios for:
  - no selection baseline;
  - raw data;
  - Published range;
  - 5–95% range with outer extent;
  - manual entry;
  - narrow/resized dialog;
  (the Library grouped/multi-select scenarios already exist at the baseline;
  refresh them if this plan's stages changed their output)
- inspect light/dark and at least Norwegian where text width matters;
- remove obsolete wording such as `Range only` from this dialog when
  `Published range` is the correct new label;
- update this plan with final deviations and manual-test result.

### Required regression matrix

At minimum rerun:
- `tests/test_reference_display.py`;
- `tests/test_add_reference_library_list.py`;
- Stage 1 preview tests;
- Stage 2 Community/My observations tests;
- Stage 3 manual tests;
- `tests/test_community_search_input.py`;
- `tests/test_my_observations_genus_browsing.py`;
- `tests/test_library_taxon_scope_hint.py` if the control still exists, or its
  explicit replacement test if superseded;
- relevant full `tests/test_add_reference_dialog.py`;
- `tests/test_reference_entry_editor.py`;
- `tests/test_reference_editor_reported_statistics.py`;
- renderer/screenshot inventory tests;
- translation validation;
- `py_compile`;
- `git diff --check`.

### Final human acceptance

1. Library relevance grouping and counts.
2. Selection vs checkbox behavior.
3. Add-N footer.
4. Source row information hierarchy and badges.
5. Fixed comparison axes while changing rows.
6. No fabricated median/mean.
7. Explicit 5–95 stays distinct from published/typical range.
8. Raw spores tab never invents rows.
9. Community genus search still works live.
10. My observations genus browsing still works.
11. Manual parse workflow and explicit keyboard order.
12. Publication editor cancels on first click.
13. Dialog shrinks and re-expands cleanly.
14. Save-to-library behavior, if the Stage 4 dependency has landed.
15. nb_NO/sv_SE/de_DE visible strings.

---

# Scope and done

## Scope cut if this grows too large

Cut polish before semantics.

Safe candidates to defer:
- advanced animation/transitions;
- visual embellishments beyond the approved hierarchy;
- cross-tab accumulation of checked sources;
- sophisticated overflow indicators beyond an honest non-silent marker;
- making Community/My observations visually identical to Library when their
  data shape does not justify the same row chrome.

Do **not** cut:
- scientific display semantics;
- no-selection observation baseline;
- honest Published range versus 5–95 labeling;
- no fabricated statistics/raw points;
- PR #6 regressions;
- localization of new visible strings.

## Definition of done

The redesign is done only when the contract can be checked requirement by
requirement, the source semantics survive round-trip into the preview and
storage, the PR #6 behaviors still work (or are explicitly superseded with
replacement tests), and no final UI state depends on unlanded historical branch
behavior.
