# Add reference dialog redesign

Status: active planning/implementation.
Branch: `feature/add-reference-dialog-redesign`.
Base at plan creation: `main` merge `a1029d95b2abdd1fcbd6b236d8e80827825bb8a6`
(PR #6, reference-discovery/cloud-account UI cleanup).

Canonical design contract:
`docs/design/DESIGN-CONTRACT-add-reference-dialog.md`

This plan redesigns the existing Add reference dialog around the approved
comparison-first mockups without reintroducing unrelated historical branch
ancestry.

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

---

## Current implementation findings

These findings were checked against `main` at `a1029d9`.

### Existing dialog seams to preserve

`ui/add_reference_dialog.py::AddReferenceDialog` already owns one dialog with
the four required source tabs and a shared `ReferencePreviewPane` in a
horizontal splitter. The redesign should build on that structure, not create a
parallel dialog.

The Library tab currently uses:

- `filter_library_candidates(...)`
- `MeasurementSetRepository.list_attachment_candidates(...)`
- one `QListWidget`
- `TwoLineRow`
- one `_selected_candidate`
- `_populate_results_list()`
- `_populate_preview(...)`

That is the natural seam for the list/grouping/multi-select work.

The preview is already isolated in `ui/reference_preview_pane.py`. It currently
uses a 3x4 summary table plus Raw spores / Method / Calibration / Provenance
tabs. The visual comparison can therefore be replaced inside the existing
shared preview component instead of duplicating preview logic in each source
tab.

The manual tab already embeds `ReferenceEntryEditor` inside a scroll area and
reuses the existing parser and normalized submission path. Preserve that reuse.

### Scientific model: no new schema migration is currently indicated

The stored `MeasurementSet` already has:

- outer Length/Width/Q pairs;
- inner/core Length/Width/Q pairs;
- scalar Length/Width/Q means;
- raw points;
- `measurement_details_json`;
- sample/method/provenance fields.

The frozen measurement-content contract already represents explicit range
meaning. In particular, a metric `core_range` can be tagged as
`percentile_interval` with explicit `percentile_bounds`, while ordinary
reported/typical ranges remain distinct.

Therefore the UX distinction `Published range` versus `5–95% range` should be
implemented as a projection of existing stored semantics, not by inventing a
new database enum or migration.

This conclusion must be rechecked in Stage 1 against all four source paths
(Library, Community, My observations, manual entry). If one path cannot carry
the semantics without loss, stop and report the exact boundary before changing
schema.

### Important projection gap

`MeasurementSetCandidate` currently exposes lightweight chooser metadata such
as `data_kind`, `raw_text`, taxon/source labels and IDs, but not the complete
measurement-content semantics needed to label a row honestly as
`Published range` versus `5–95% range`.

Stage 1 must solve that without an N+1 query per visible row. Prefer a batched
or joined projection, or another narrow solution that makes the list semantics
available efficiently.

### Separate historical dependency: Save to library

Current `main` deliberately does not contain the separate manual
`Save to library` picker flow from the preserved
`feature/reference-save-and-plot` work (`4efdf7d` historical candidate).
That work also contains failure-propagation behavior deliberately excluded from
PR #6.

The redesign must **not** silently re-create fragments of that historical
feature just because the mockup contains a `Save to library` button.

Before the final manual-entry/footer stage, either:

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

## Stage 0 — Commit the design contract and pin the baseline

**Goal:** make the visual/behavioral target durable before production code
moves.

### Work

- Add `docs/design/DESIGN-CONTRACT-add-reference-dialog.md`.
- Add this active plan.
- Record the base (`a1029d9`) and the approved mockup locations if the image
  files are added to the repository.
- Inventory the exact current tests covering N29–N33.
- No production-code changes.

### Verification

- `git diff --check`
- plan and contract contain no machine-specific worktree paths as durable
  implementation references other than this branch/base record;
- no source/test/i18n changes.

### Exit criterion

A later agent can understand the target and stage ordering without access to
the original chat.

---

## Stage 1 — Scientific display semantics and chooser projection

**Goal:** create one tested, non-Qt interpretation of stored reference content
for list badges and comparison rendering before changing visible layout.

### Work

Introduce a small pure display-semantics projection (exact module/name chosen
after targeted inspection), conceptually producing per source:

- source kind / availability of real raw points;
- outer range per metric, if explicitly present;
- core/inner range per metric, with its explicit meaning;
- explicit percentile bounds;
- explicit scalar mean;
- explicit median / median interval;
- mean interval;
- sample size;
- flags saying whether any displayed statistic is reported versus derived;
- compact data label:
  - `Raw data`
  - `Published range`
  - `5–95% range`
  - exact other percentile label where needed, e.g. `10–90% range`.

Rules:

1. `Raw data` requires actual individual point data. A `data_kind` string alone
   must not manufacture raw-data semantics.
2. `5–95% range` requires an explicit percentile interval with bounds 5 and 95.
3. Ordinary/typical/reported range semantics without raw points use
   `Published range` as the compact badge while preserving their more precise
   internal descriptor.
4. Never derive mean/median from a midpoint.
5. Unsupported future `measurement_details_json` remains inspect-only; do not
   guess semantics from an unknown version.
6. Parmasto is provenance/method, not a separate data kind.

Extend the Library candidate-loading path so the above projection is available
without one database lookup per row. Keep the repository API narrow.

Audit all source paths:
- Library measurement sets;
- Community results;
- My observations;
- manual/parser output.

Do not change schema unless this audit proves an actual representational gap.

### Tests

Add focused pure tests covering at least:

- raw points;
- ordinary published range;
- outer min/max + explicit 5–95 core range;
- Parmasto-style stored content;
- explicit non-5/95 percentile interval;
- typical/core range with no percentile descriptor;
- explicit mean and median;
- no centre statistic;
- legacy untagged row;
- unsupported future details version.

### Verification

- focused semantics/projection tests;
- existing measurement-content contract tests;
- `py_compile` for touched modules;
- `git diff --check`.

### Human gate

None required if this stage has no interactive UI change.

### Exit criterion

Every later UI stage can ask one projection what the source actually contains;
no widget needs to reinterpret database fields independently.

---

## Stage 2 — Library row anatomy, relevance grouping and multi-select

**Goal:** implement the left side of the approved Library mockup while keeping
the existing preview functional.

### Work

Refactor the Library list around two independent states:

- `preview_selection`: exactly one row shown on the right;
- `checked_sources`: zero or more rows that will be added to the plot.

Required behavior:

- group in the fixed order `This taxon` → `Same genus` → `Rest of library`;
- group headings show counts; empty groups disappear;
- taxon is the italic primary line;
- citation/source is secondary metadata;
- measurement is right-aligned monospace and protected from truncation;
- show relevance badge plus semantic badge from Stage 1;
- checkbox and row selection remain independent;
- checking a row also makes it the preview row;
- row selection alone does not check it;
- custom green selected-row treatment; no Qt blue highlight;
- `Only this taxon` and text search continue to compose predictably;
- search/filter changes do not silently mutate checked state;
- changing the comparison taxon clears checked state unless a narrower
  demonstrated rule is safer;
- `+ New publication…` remains an action row, never a checkable source.

The design contract requires multi-select for the Library list. Do not broaden
this stage into cross-tab accumulation unless the approved contract is amended.

### Add-to-plot behavior

Define partial-failure behavior before wiring the button. The current callbacks
attach one source at a time and do not provide transactional multi-attach.

Required minimum:
- deterministic order;
- no source attached twice;
- if one attach fails, do not pretend the whole batch succeeded;
- keep the dialog open and preserve enough state to retry failed sources;
- already-successful attachments must be reported honestly rather than rolled
  back in the UI if the persistence layer did not roll them back.

If the existing callback cannot report failure at all, stop and report that
contract gap rather than inventing success semantics.

### Footer

- `Add to plot` when zero/one according to existing enabled-state rules;
- `Add N to plot` for N > 1;
- helper text says `N sources selected`, not `ready`.

### Tests

Pin:
- relevance grouping and counts;
- empty groups hidden;
- taxon/citation/measurement row model;
- independent selected vs checked state;
- checkbox selects row;
- row click does not check;
- checked state under search/filter;
- taxon-target reset;
- footer count;
- semantic badges;
- no default-blue selection styling where testable;
- single-Cancel publication regression.

### Verification

Run:
- new Library-list tests;
- `tests/test_library_taxon_scope_hint.py`;
- relevant `tests/test_add_reference_dialog.py`;
- `py_compile`;
- `git diff --check`.

### Human gate

Manual:
1. verify grouping/row anatomy with enough rows to fill all three groups;
2. check two rows, preview a third un-checked row, confirm `Add 2 to plot`;
3. search/filter and return; checked sources are not silently changed;
4. `+ New publication…` still closes with one Cancel;
5. shrink and re-grow the dialog.

### Exit criterion

The left pane matches the approved information hierarchy and the old preview
continues to work.

---

## Stage 3 — Comparison-first Summary and honest Raw spores states

**Goal:** replace the old summary table with the approved comparison
visualization, using Stage 1 semantics rather than widget-local inference.

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
- Stage 1 tests;
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

## Stage 4 — Community and My observations on the new preview contract

**Goal:** adapt the two non-Library source tabs to the Stage 3 comparison
model without broadening scope unnecessarily.

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

## Stage 5 — Manual-entry layout and semantic preview

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
- manual preview uses the Stage 3 comparison model live;
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

## Stage 6 — Manual Save to library footer integration

**Blocked until the separate save/plot behavior is deliberately landed or
re-authorized.**

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

## Stage 7 — Localization, screenshot parity and acceptance

**Goal:** finish the visual contract without hiding semantic or regression debt.

### Work

- run repository localization workflow;
- translate all new/changed strings in nb_NO, sv_SE and de_DE;
- add/update deterministic review scenarios for:
  - Library grouped/multi-select;
  - no selection baseline;
  - raw data;
  - Published range;
  - 5–95% range with outer extent;
  - manual entry;
  - narrow/resized dialog;
- inspect light/dark and at least Norwegian where text width matters;
- remove obsolete wording such as `Range only` from this dialog when
  `Published range` is the correct new label;
- update this plan with final deviations and manual-test result.

### Required regression matrix

At minimum rerun:
- Stage 1 semantic tests;
- Stage 2 list tests;
- Stage 3 preview tests;
- Stage 4 Community/My observations tests;
- Stage 5 manual tests;
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
14. Save-to-library behavior, if Stage 6 dependency has landed.
15. nb_NO/sv_SE/de_DE visible strings.

---

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
- Library relevance grouping;
- Library multi-select;
- selection/checkbox separation;
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
