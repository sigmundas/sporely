# Reported statistics and explicit range semantics

The executable stage definitions of this plan are the `## Stage <label> — …`
sections under *Canonical stage sequence* below (Stage 1 → 2 → 3A → 3B → 3C →
3D → 4 → 5). The `… handoff` sections that follow this paragraph are records of
stage execution, newest first; they define nothing. The first one is the
**current stage**; the rest are historical and kept verbatim.

## Stage 5 handoff — 2026-09-17 (current stage; review passed, gates stay closed, landing narrowed)

Status: **Stage 5 independent review complete. Merge approved in both
repositories. Both rollout gates remain CLOSED. The plan stays open.**

Stage 5 owns two separate decisions and they resolved differently: the
candidates are correct and safe to land, but neither gate may open yet. Landing
this work therefore activates nothing.

### Reviewed candidates

- `sporely-py`: frozen Stage 4 candidate
  `0c43a1bb33afe3f6e568786c6eda8bbb27456c08`, base
  `12992e61630c325dabd61817ddc2d8ed1fb81e00`. The two commits above it
  (`24f9fd0`, `e3c2ddc`) were verified documentation-only — `git diff --stat`
  touches this plan file alone — so the human gate was run against content
  still present at HEAD.
- `sporely-web`: frozen candidate
  `1bb5c804833bcdfff6b7f05c37395c1abd4e2de9` on
  `feature/reported-statistics-cloud-transport`, which is exactly that branch's
  tip. No migration landed on `sporely-web` main since its base.

### Verdicts

Merge safety: **APPROVED** for both candidates. No privilege widening, no
migration collision, and the old-client RPC guard has no bypass — the public
wrapper forwards the payload unmodified, `authenticated` holds only `SELECT` on
`reference_measurement_sets`, and the RPC is the sole writer to that table.

Activation: **both gates KEEP CLOSED.** The reason is structural rather than a
missing checklist item. `MINIMUM_SUPPORTED_READER_VERSION_GATE_OPEN` requires
that every supported desktop can read snapshot v2, but the v2 reader exists only
on this unmerged branch, so no released build can read v2 and there is by
definition no deployed reader population.
`MINIMUM_SUPPORTED_DESKTOP_VERSION_GATE_OPEN` fails for the same reason: no
released desktop carries the Stage 3A write barrier. Both gates can only be
reconsidered after this work merges and ships. There is client-version
telemetry (`profiles.last_app_version`, `client_activity_daily`) but no
server-side minimum-version enforcement on the reference RPCs; telemetry is
observation, not a gate.

### Narrowed landing

The Stage 4 candidate sat on a branch that had accumulated two unrelated
efforts. Landing was narrowed to a clean branch cut from `main` (`2e73578`)
carrying only this plan's commits, the contiguous range `8097bc8..0c43a1b` plus
the two Stage 4 documentation commits.

A review pass on 2026-09-17 accepted the contract implementation itself and sent
back one remaining scope leak, now removed. The first transplanted commit
(`09afc99`) had combined this plan's documents with agent-sparring project
configuration. `.sparring/project.toml` hard-coded a machine-specific test
command (`/Users/<user>/.../sporely-py/.venv/bin/pytest`), and
`.sparring/PROJECT.md` recorded branch and worktree state as durable project
knowledge — naming a canonical checkout on `feature/reference-save-and-plot`, a
linked worktree for `feature/reported-statistics-contract`, and a frozen
`8097bc8` test baseline from 2026-09-11. On `main` all of that is stale the
moment branches move, and would mislead a future agent. Both files were removed
along with the `.gitignore` entries that existed only to support them, leaving
`.gitignore` byte-identical to `main`. Both plan documents in
`docs/plans/active/` were kept. Agent-sparring can be adopted later as its own
infrastructure change with a portable configuration.

Deliberately excluded and preserved on `feature/reported-statistics-contract`,
not dropped:

- The cloud-sync facade extraction (`ea55c68`, `4dc6b82`, `6e05f31`, `8d06a9d`
  and related), which moves ~1529 lines out of `utils/cloud_sync.py` into
  `utils/cloud_sync_impl/*`. It belongs to
  `docs/plans/active/2026-08-23-cloud-sync-extraction.md`.
- `4efdf7d` "Fix reference identity and separate library saving from plotting",
  which belongs to `docs/plans/active/2026-09-10-reference-save-and-plot.md` and
  carries the `ui/main_window.py` change hiding the reference-shape setting.

One genuine dependency was found and is recorded rather than used as grounds to
re-include the refactor. Stage 3A's `68d1855` needs two pure helpers,
`_normalized_index_ddl` and `_existing_index_sql` (~17 lines of `sqlite_master`
index comparison in `database/reference_library_schema.py`), which had been
committed inside the cloud-sync prestage baseline repair `ca16130`. They are
reference-library schema code, not cloud-sync code, and only those two functions
were transplanted. Nothing else from the cloud-sync effort is present:
`utils/cloud_sync_impl/` does not exist on the narrow branch, `ui/main_window.py`
is byte-identical to main, and `utils/cloud_sync.py` differs from main by one
line — Stage 3C adding the three extension columns to the cloud select list.

Result against `origin/main` (`2e735780683f70bb2a5d0f305757e0be945071f6`):
**64 files changed, +15998/-1118, 32 commits** on branch
`feature/reported-statistics-narrow`, down from 93 files, +21463/-4247 and 42
commits on the wide branch. These are the numbers as of the scope-leak cleanup
commit that carries this line, the final commit of this pass.

The commit count grew from the 25 first reported, in three steps. The initial
transplant was the 24-commit range `8097bc8..0c43a1b` plus one translation
regeneration commit, giving 25. Cherry-picking the two Stage 4 documentation
commits `24f9fd0` and `e3c2ddc`, which fall *after* the frozen candidate and so
were outside the transplanted range, plus this Stage 5 handoff record, brought
it to 28. The permanent Add plot regression test and two handoff updates made
30, and the scope-leak cleanup described below makes 32. No production commit
was added after the transplant: every commit beyond the original 25 is
documentation, translation regeneration, test or scope removal.

### Verification

Project venv, `QT_QPA_PLATFORM=offscreen`.

- Write barrier, exercised directly against a real SQLite file: a
  contract-aware connection initializes the schema, both guard triggers and all
  three extension columns are present, and it inserts normally. An unaware
  connection is refused on INSERT and UPDATE with
  `no such function: sporely_measurement_contract`, can still SELECT, and can
  still DELETE (deliberately outside the barrier). Registering the contract on
  that same connection makes the INSERT succeed, confirming the barrier is
  registration-based and not data corruption.
- Reported-statistics and reference suites: **564 passed**.
- iNaturalist and publish-media suites from main: **156 passed**.
- Sweep `-k "reference or measurement or curated or legacy or add_reference"`:
  **1345 passed, 2 skipped, 3 errors** — the same three pre-existing baseline
  errors (two taxonomy release-dir, one `scripts` package-name collision under
  the sweep's import order).
- Real manual-reference / Add plot workflow, driven through
  `QuickAddReferenceService.create_and_attach`: a manual "Danmarks
  basidiesvampe" spore-size reference creates work, treatment and measurement
  set, attaches to the observation, persists the legacy-only projection with all
  three extension columns NULL, and freezes a v1 snapshot carrying no enhanced
  content — correct behavior under a closed reader gate. This is now a permanent
  regression, `tests/test_reference_add_plot_workflow.py` (3 cases), rather than
  a throwaway check; it is the suite that would have caught the reported
  "Could not add the library reference: no such function:
  sporely_measurement_contract" failure.
- Cloud-sync facade verified against **main's** implementation: `utils.cloud_sync`
  resolves to the 26188-line module, `cloud_sync_impl` is never imported, and
  `ui.observations_tab`, `ui.main_window`, `utils.publish_media` and
  `utils.artsobs_uploaders` all import cleanly.

### Merging this branch deploys nothing

Checked explicitly, because finding 1 below would become merge-blocking if a
merge to `main` applied a migration. It does not, in either repository:

- `sporely-py` has no `supabase/` directory and no migration or `.sql` file
  anywhere in this branch's diff; its single workflow `.github/workflows/release.yml`
  triggers only on `v*.*.*` tags, not on a push to `main`, so merging does not
  even build a desktop release.
- `sporely-web` has only `release-android.yml` (tags plus manual dispatch) and
  `supabase-heartbeat.yml` (cron plus manual dispatch). Neither runs
  `supabase db push`, `supabase migration up` or `supabase link`; no CI step
  applies a migration at all.

Migrations reach production only through a deliberate manual `supabase db push`.
Finding 1 therefore blocks that push, not this merge.

### Outstanding

1. **Public snapshot forwards unvalidated future versions.**
   `private.reference_measurement_details_valid` returns true for any non-1
   `schema_version`, and `private.public_reference_snapshot` forwards the object
   verbatim to RPCs granted to `anon`. Deviates from contract sections 3 and 9.
   Must be fixed before `supabase db push`, not before merge.
2. **Gate coupling.** Opening the reader gate alone lets the editors create
   enhanced rows, which then trip the still-closed export gate and break bundle
   and portable export (`utils/db_share.py:414`,
   `utils/archive/portable_export.py:62`, neither caught in production code).
   Open both together, or add UI-level handling first.
3. **Gate messages are not translatable** (`references/measurement_content_gates.py`),
   unreachable while both gates are closed.
4. **31 untranslated iNaturalist strings** per language, already untranslated on
   main and not regressed here.
5. **Deployment ordering, hard constraint.** `sporely-web` migration
   `20260913120000` must be pushed and verified before any desktop build
   carrying the Stage 3C adapter is released: that adapter sends all three new
   keys on every measurement-set payload and a pre-migration server rejects all
   of them via the unknown-keys allowlist. Independent of both gates.
6. Before pushing, confirm production's two CHECK constraints carry the assumed
   auto-generated names; a mismatch aborts `20260914090000` at
   `DROP CONSTRAINT`.

### Branch state at the end of this pass

Branch `feature/reported-statistics-narrow`, cut from `main` at
`2e735780683f70bb2a5d0f305757e0be945071f6`. The final SHA is the commit that
carries this section — a commit cannot contain its own hash, so read it with
`git rev-parse feature/reported-statistics-narrow` rather than from this file.
The last commit to touch production or test code is
`b68858fe0845d6ff80305d1ebbf5b8c15ec96621`, the Add plot regression; everything
after it is this plan and the scope-leak removal. Every excluded commit
remains reachable on `feature/reported-statistics-contract`,
`feature/cloud-sync-transport-boundary`, `feature/reference-save-and-plot` and
`review/cloud-sync-prestage-2026-09-08`; nothing was dropped.

### Why this plan stays open

Stage 5's activation decision was to keep both gates closed, so the plan's own
objective — v2 emission and enhanced attachments in the field — is not yet met.
The plan reopens for an activation pass once this work has shipped and the
reader population can be established. `docs/plans/active/2026-09-09-reference-measurement-table-parser.md`
likewise stays open: it defers its Stage 2 to this plan and states explicitly
that parser-stage completion is not completion of the overall request.

## Stage 4 handoff — 2026-09-15 (candidate pushed, human gate passed)

Status: **Stage 4 implemented, human gate passed, candidate committed and
pushed.** Editor and UI inspection with guarded editing: the typed parser
output now reaches both editors, compact meaning tags carry an accessible
explanation, the mean field accepts a scalar *or* an interval, reported median
and S.D. appear beside the mean and never inside it, a reported Q mean no
longer lands in the Parmasto species-mean field, and the library manager
presents enhanced content read-only under a closed gate while preserving it
untouched. The stage was human-gated under AGENTS.md (interactive edit / swap /
save / restart); those checks were run by the branch owner on 2026-09-15 and
recorded below, which is what released the candidate for commit. Acceptance and
merge remain the branch owner's separate decisions.

- Stage id: `stage-4-editor-and-ui-inspection-and-guarded-editing` (brief,
  handoff and notes in `.sparring/stages/<that id>/`).
- Branch `feature/reported-statistics-contract`, base
  `12992e61630c325dabd61817ddc2d8ed1fb81e00`, candidate
  **`0c43a1bb33afe3f6e568786c6eda8bbb27456c08`**, pushed. The commit that adds
  this status record is a documentation-only follow-up; it does not alter the
  frozen candidate, whose production, test and translation content is exactly
  what the human checks were run against.
- Both rollout gates are committed **closed**. Stage 5 still owns the decision
  to open either one.

Files changed: `ui/measurement_content_view.py` (new),
`ui/reference_entry_editor.py`, `ui/reference_library_manager_dialog.py`,
`ui/reference_preview_pane.py`, `references/measurement_content_gates.py`,
`tools/review_ui/scenarios/references.py` (two new scenarios),
`tools/update_translations.sh`,
`i18n/Sporely_{nb_NO,sv_SE,de_DE}.{ts,qm}`; tests
`tests/test_measurement_content_view.py` (new, 50 cases),
`tests/test_reference_editor_reported_statistics.py` (new, 34 cases),
`tests/test_render_review_screenshots.py` (two registered scenario ids); this
plan. Production code changed: yes. Tests changed: yes. No schema, cloud,
sync, snapshot, parser or repository change.

**The guard, and why it is the reader gate.** Everything the entry editor
creates is bound for an observation attachment, and
`_gated_observation_reference_snapshot` refuses to freeze an enhanced row as
evidence while the reader gate is closed. Storing enhanced content from this
editor would therefore only move the refusal to the save button — and because
the Stage 2 parser tags *every* inner range, that would break the ordinary
paste-and-attach flow for essentially every source. New accessor
`enhanced_editing_enabled()` in `references/measurement_content_gates.py` reads
the same reader gate from the editors' side (no new switch). While it is closed
both editors *show* the tags, the reported median/S.D. and the interval mean,
state plainly that this version does not store them yet, and persist the
legacy-only projection — byte-for-byte what the same source produced before
this contract existed; the manager additionally hides the Q core inputs it
cannot write, because offering a field this version cannot persist would be a
lie. With the gate open the same code paths write `measurement_details_json`
and the Q core pair. Decision the reviewer should
challenge: the accessor mirrors the *reader* gate only, not the
minimum-supported-desktop bundle-export gate, because the reader gate is the
one the attachment boundary actually enforces; requiring both would invent a
fourth policy the contract does not define.

**Parser → content wiring (the Stage 3B deferral), in both editors.** One
function owns the transition rules —
`ui/measurement_content_view.py::fold_metric_inputs` — and both editors are
adapters over it, so there is genuinely no second entry workflow. Plain values
move by assignment, but every *transition* goes through the frozen Stage 2
operations: `clear_pair` when a described pair is emptied,
`clear_statistic` / `set_scalar_mean` / `set_mean_interval` for the mean (via
`apply_mean_cell`), and `swap_length_width` on L↔W so a `5%-95%` or
`reported extremes` tag can never be left describing the other dimension.
`ReferenceEntryEditor._set_parsed_result` keeps `result.to_content()` as the
editor's working state; `_MeasurementSetForm` keeps `self._parsed_content` and
`_extension_updates` folds the form's values into it, so a parsed manager entry
persists its interval means, medians, standard deviations, range tags and Q
core pair. The manager gained the Q core inputs that mapping needs (shown only
while the gate is open, since a field this version cannot persist must not be
offered) and its mean fields now take a scalar or an interval. Content written
by a newer version is never rewritten: the fold returns it untouched and the UI
says so instead of showing tags it cannot interpret.

**Correction is retraction, not assertion.** The tag strip's control is a menu:
per-metric "drop the range interpretation" for each metric that carries one,
plus "Discard all reported statistics". Both keep every measured number; the
per-metric form also keeps that metric's reported median and S.D. Asserting a
*different* descriptor kind is deliberately absent — contract section 3
enumerates five edit operations and adding a sixth is a Stage 1 amendment, the
plan defers "editable advanced details", and on the merits a field that lets a
reader type in a percentile the source never printed manufactures exactly the
evidence this contract exists to keep honest.

**Defects this stage fixes.** Both editors filled their mean field from the
parser's `p50` centre and dropped `result.length_mean` / `width_mean`
entirely, so a headed table's reported mean was lost; both now prefer the
source's own scalar mean and keep `p50` only as the legacy stand-in for the
compact `a-b-c` form (a table's *median* cell never reaches `p50`, so no
median can land in a mean field). `_set_parsed_result` wrote a parsed
`Qm`/`Qav` into the Parmasto species-mean widget; it now goes to the Q row's
Mean cell, which already mapped to `q_mean`. The min/max tab overlapped its
own widgets once the tag strip was added and is now a `QScrollArea` with a
three-row minimum on the table. The tag strip and the preview pane's new line
inherit the theme foreground instead of a hardcoded slate that was unreadable
in dark mode. `ReferencePreviewPane.set_summary` now resets the
reported-statistics line, because the pane is shared between the picker's tabs
and a tagged literature entry would otherwise leave its tags describing a
community dataset's numbers.

**`_summary_interval` — already resolved, nothing to fix.** The name exists
only in this plan; it belonged to the superseded `SummaryStatistics` prototype
still sitting uncommitted in the canonical checkout. Stage 2 replaced that
scope with `ScalarStatistic` versus `IntervalStatistic`, which preserve the
shape by construction (`_parse_statistic` never collapses `9.2-9.2`). Stage 4
carries the distinction the rest of the way: `parse_mean_cell` /
`format_statistic` round-trip it through the editor, asserted directly.

**Library manager under a closed gate.** Sparring round 1 was right that
inspect-only presentation does not waive the wiring above; both are now
implemented and the gate decides which runs. With the gate open the form saves
the full typed content. With it closed the form shows a stored set's tags and
reported median/S.D. read-only and preserves the extension by simply not
sending those columns — an omitted key leaves the stored value alone, so an
ordinary edit is byte-identical — and owns exactly one transition, removal:
`clear_pair` when the user empties a column pair a tag describes, because
otherwise the repository would refuse the write citing a column the form does
not display. Converting a set to `raw_points` clears the extension along with
the aggregate numbers it already discarded. Under a closed gate the manager
never *creates* enhanced content.

Verification (project venv): `py_compile` on every touched module clean;
`git diff --check` clean; the two new test modules 84 passed; the reference-area
sweep (`-k "reference or measurement or curated or legacy or add_reference"`,
which covers both editors, the parser and the contract module) 1328 passed with
only
the two baseline taxonomy release-dir errors. The full suite
(`QT_QPA_PLATFORM=offscreen pytest -q -p no:cacheprovider
--continue-on-collection-errors`, 2 min 45 s) is **33 failed, 4365 passed, 10
skipped, 55 errors** against Stage 3D's 33 / 4281 / 10 / 55 — the same failing
and erroring module set (PROJECT.md baseline items 1-4 plus the two
`test_cloud_visibility_phase7.py` push cases Stage 3D verified pre-date it),
and the +84 are exactly this stage's two new test modules (50 + 34).
`tests/test_render_review_screenshots.py` still shows its documented
four baseline failures: two from the missing worktree `.venv`, two from two
scenario ids (`reference.add-dialog-manual-saved`, `-dark`) that were already
unregistered before this stage. Both of this stage's scenarios
(`reference.reported-statistics`, `reference.measurement-set-form-enhanced`)
are registered, so this change adds no new drift.

Screenshots (layout only, per `.claude/rules/ui-screenshots.md`): new scenario
`reference.reported-statistics` renders the 5%-95% headed table with
`9.2-11.7` in the Mean column for Length, `5.6-6.7` for Width and `1.55-1.78`
for Q; the tag strip reads `L extremes: reported · L inner 5–95% · L mean:
interval · …`; the "Also reported:" line reads `L median 9.2-11.7 · L S.D. 0.6
· …`; the amber notice and the *Discard reported statistics* button are both
visible. `reference.nb-no` shows the Norwegian strings (`L ytterverdier:
oppgitt · L indre: uspesifisert · …`) fitting on two lines.
`reference.add-dialog-manual-range-dark` shows both tag strips legible in dark
mode and `Qm = 1.89` in the Q row's Mean cell rather than the Parmasto tab.
`reference.measurement-set-form-enhanced` patches the reader gate open for that
scenario alone and shows the library-manager form the corrected save path
fills: the Q row reading `1.3 | 1.42 | 1.55-1.78 | 1.96 | 2.07`, interval means
in the Length and Width mean fields, and the "Found in the expression:" line.

Localization: `ui/measurement_content_view.py` added to
`tools/update_translations.sh`. Each of the three `.ts` files carries 42 more
messages than at the stage base (2332 → **2374**), covering the new
`MeasurementContent` context, the added editor strings and the round-1
correction strings; every one is translated into nb_NO, sv_SE and de_DE and
compiled. `lrelease` reports **2374 finished, 0 unfinished** in all three
languages. The module calls `QCoreApplication.translate` with the literal
context at every site on purpose — a shorter local wrapper hid every string in
it from `lupdate`, which was caught and fixed during this stage.

**Sparring round 1 returned `SEND_BACK`; both findings are implemented.**
(1) The library manager did not build its set from `to_content()` — the fix is
the shared fold above, plus the Q core inputs and interval-capable mean fields
it needed. (2) There was no explicit correction control for a wrong range
interpretation — the fix is the retraction menu above. Round 2 returned
`READY` on the implementation, and round 3 sent back documentation only: this
handoff still read "uncommitted / Candidate SHA: none yet" after the candidate
was pushed, which is what the follow-up commit carrying this text corrects.
Full response in the stage's `sparring.md`.

**Human gate — passed (branch owner, 2026-09-15).** Five blocking checks, all
reported Pass, run against content identical to the candidate. Screenshots
prove layout only; none of these interactions is visible in one.

1. `compact-editor-edit-swap-save-restart` — with both rollout switches closed,
   a synthetic tagged table (interval means, median, S.D. and Q) pasted into
   the compact editor, values edited, length and width swapped, the set saved
   and attached, the application closed and reopened, and the saved attachment
   inspected.
2. `reported-statistics-retraction` — tagged content parsed, then each
   available per-metric "drop the range interpretation" action used, then
   "Discard all reported statistics"; fields and save results inspected after
   each.
3. `library-manager-closed-gate` — with the reader gate closed, an existing
   enhanced measurement set opened in the library manager, an ordinary field
   edited and saved, the row reopened; and a synthetic enhanced table parsed
   into a new manager form.
4. `library-manager-open-gate-roundtrip` — with
   `MINIMUM_SUPPORTED_READER_VERSION_GATE_OPEN` set true locally, a synthetic
   table carrying explicit percentile/core and extreme bounds, interval means,
   median, S.D. and Q core values parsed and saved through the library
   manager, then restarted and reopened; the switch restored to false before
   committing, and confirmed `False` in the candidate.
5. `ui-wording-and-layout` — both editors inspected in light and dark themes
   and in Norwegian, with content long enough to wrap the tag and
   reported-statistics lines, and the controls navigated by keyboard and
   accessibility tooling.

This list is the durable record: the stage directory under `.sparring/stages/`
is gitignored and its files are regenerated by the sparring tooling, so the
operator checklist those checks were run from no longer exists verbatim. The
per-check Pass verdicts are in that directory's `sparring.md` for as long as it
survives.

Deferred, deliberately: the parsed sample size `n` still does not reach
`normalized_measurement_set_payload` (pre-existing; the payload derives
`sample_size` from raw points only). `_render_measurement_preview` still
prints untranslated `L`/`W`/`Q` prefixes, so the Norwegian build shows `W` in
the parse line beside `B` in the tag strip. Editable advanced details,
percentile-bound correction and a descriptor-kind control remain out of scope
— the frozen Stage 2 API has no descriptor-set operation, and the plan defers
richer authoring.

Subagents: none used.

## Stage 3D handoff — 2026-09-14 (candidate `4e451abd`, sparring round 2 READY)

Status: **Stage 3D implemented and self-verified in both repositories.**
Snapshot version 2, version-aware v1/v2 comparison, readers accepting v2
before anything emits it, preserve-or-reject on every transfer path, and the
two rollout gates as explicit reversible switches, shipped closed. Stage 3C
(accepted at `3c0f65b5` / `b32eb922`) is the base in both repositories.
Nothing Stage 1, 2, 3A, 3B or 3C froze was reopened; the parser, the editors,
plotting and matching are untouched, and no enhanced attachment or enhanced
editing is activated. Merging either branch, and deploying the migration,
remain the branch owner's separate decisions.

- Stage id: `stage-3d-snapshot-v2-and-attachment-export-import-transport`
  (brief, handoff and notes in `.sparring/stages/<that id>/`).
- `sporely-py`: branch `feature/reported-statistics-contract`, base
  `3c0f65b5cf43f9ee7d08db4d2c9f8ea12478ecc5`, desktop candidate
  **`4e451abd5c113747a5b49382b2efc1011f42a8a3`**, pushed. The commit that adds
  this handoff records the cloud slice and the aligned contract text; it does
  not alter the frozen desktop candidate.
- `sporely-web`: branch `feature/reported-statistics-cloud-transport`, base
  `b32eb92214f6eae9d308baa17128a53b83b6a896` (the accepted Stage 3C
  candidate), candidate **`1bb5c804833bcdfff6b7f05c37395c1abd4e2de9`**,
  pushed. Worked in the linked worktree `sporely-web-reported-statistics`,
  as in Stage 3C, because the main checkout carries unrelated uncommitted
  work on another branch; the SQL files were authored and run from the main
  checkout and then copied into the worktree byte-identically, and the
  untracked copies were removed from the main checkout.

Files changed — `sporely-py`: `references/measurement_content_gates.py` (new),
`database/reference_citation.py`, `database/reference_library.py`,
`database/reference_library_schema.py`,
`database/reference_use_sync_reconciliation.py`,
`database/curated_reference_forks.py`, `utils/db_share.py`,
`utils/archive/portable_export.py`, `utils/archive/portable_import.py`,
`docs/supabase-sync-contract.md`; tests
`tests/test_reference_snapshot_v2_transport.py` (new, 32 cases) and
`tests/test_reference_measurement_content_persistence.py` (one Stage 3B
boundary assertion, see below); this plan. Production code changed: yes.
Tests changed: yes.

Files changed — `sporely-web`:
`supabase/migrations/20260914090000_extend_reference_snapshots_to_version_2.sql`
(new), `supabase/tests/reference_snapshot_v2_test.sql` (new),
`supabase/tests/reference_measurement_content_extension_test.sql` (one Stage
3C boundary assertion, see below), `docs/supabase-sync-contract.md`.
Production code changed: yes (migration). Tests changed: yes.
`supabase/schema.sql` not regenerated (AGENTS.md).

Desktop ownership. `build_observation_reference_snapshot` emits version 1 for
a legacy-only measurement set and version 2 for an enhanced one; the emit rule
is a property of the row, not of a setting, so an unchanged legacy row builds
the exact snapshot it built before and no existing attachment goes stale. The
4096-byte details limit and the 65536-byte snapshot limit are enforced at
emission; a preserved future details version is embedded verbatim; a malformed
stored details object raises rather than producing a snapshot that silently
omits the statistics. `snapshot_semantic_projection` drops `schema_version`
and `reference_revision`, fills the absent extension with `None`, and returns
`None` for an unsupported version, so such a snapshot is never equal to
anything, including another copy of itself; `observation_snapshots_semantically_equal`
compares projections.

Readers ship before writers: `stage_observation_reference_use_feed`,
`curated_reference_forks._validate_snapshot` and
`portable_import._validate_reference_snapshot` accept both versions through
version-keyed *exact* key sets, so a version-2 snapshot is never intersected
down to the keys a reader happens to know and an unknown version is refused.
`copy_curated_bundle_to_personal_library` writes all three extension fields or
rejects the bundle; it validates with `mode="edit"` deliberately, because
contract section 8 excludes curated copy from opaque acceptance.

Gates. `references/measurement_content_gates.py` holds both switches, both
`False`, read at call time. The reader gate is applied at one boundary,
`reference_library._gated_observation_reference_snapshot`, through which
`_do_attach`, `snapshot_status`, `refresh_snapshot` and `successor_status`
build snapshots; while it is closed an enhanced row cannot become frozen
evidence (attach and refresh raise `ReferenceIntegrityError`,
`successor_status` returns the existing `unsupported` state and
`adopt_successor` therefore refuses). Emitting version 1 instead was rejected:
that would freeze evidence that silently omits the statistics. No new UI state
was introduced. The desktop-version gate makes `db_share.export_database_bundle`
and `portable_export.export_observations` refuse to produce an archive
carrying enhanced reference content; `full_backup.py` is deliberately not
gated, being the same user's restore path. Both refusals strip nothing, and
closing a gate again leaves every stored extension value untouched.

Cloud ownership. `private.reference_snapshot_valid` is version-keyed in both
directions; `private.reference_canonical_snapshot` emits version 2 for an
enhanced row, composed with `||` so the version-1 object is literally
unchanged; `private.public_reference_snapshot` preserves the extension it
would otherwise have rebuilt away. The curated publication CHECK and
`private.reference_curated_public_envelope` accept `1` or `2`; curation intake
gains the three keys under `measurement_set` as all-or-none rather than
required, because candidates captured before the migration legitimately lack
them, and `private.reference_curation_capture_candidate` now always emits all
three. The two replaced function bodies were copied verbatim from the
migrations that own them rather than retyped. That is directly checkable by
diffing each `CREATE OR REPLACE FUNCTION … $$;` block in `20260914090000`
against its `CREATE FUNCTION` original: `reference_curated_public_envelope`
differs in exactly two lines (the `CREATE OR REPLACE` header and
`snapshot_schema_version NOT IN (1, 2)`) and
`reference_curation_capture_candidate` in exactly the header plus the three
added keys with their comment. Nothing else in either body moved.

Boundary assertions updated, not weakened. The Stage 3B test
`test_snapshot_of_enhanced_row_is_still_the_v1_projection_of_its_ordinary_columns`
and the Stage 3C SQL assertion "v1 canonical snapshot changed for an enhanced
row" both pinned the boundary *this* stage exists to remove. Each was rewritten
to assert the Stage 3D truth and to keep what its own stage owns: that the
extension disturbs no ordinary snapshot field or measurement value, and that a
legacy row still emits its exact version-1 snapshot.

Verification — `sporely-py`: `py_compile` on every touched module and
`git diff --check` clean; the new module's 32 cases pass; the reference-area
sweep is 1439 passed with only baseline failures; the full suite is **33
failed, 4281 passed, 10 skipped, 55 errors** (run with
`--continue-on-collection-errors`, needed because the `scripts/` shadowing in
PROJECT.md baseline item 3 otherwise aborts collection). The failing set is the
documented baseline (31) plus the two
`tests/test_cloud_visibility_phase7.py` push cases, which were verified to
fail identically with the working tree checked out at the base commit
`3c0f65b5`; they pre-date this stage and are simply newer than the PROJECT.md
capture at `8097bc8`.

Verification — `sporely-web`, on the running local stack: `supabase migration
up --local` applied `20260914090000` cleanly onto a database already at
`20260913120000`; both auto-named CHECK constraints the migration drops were
confirmed against the live schema before applying
(`curated_reference_publications_snapshot_schema_version_check`,
`reference_curation_submission_versions_candidate_json_check`); the whole
`supabase/tests` suite run through `psql` is **59 passed, 1 failed**. The one
failure, `public_observation_point_prep_test.sql`, is independent: its only
RPC is `public.get_public_observation`, whose source references neither
`private.public_reference_snapshot` nor `private.reference_snapshot_valid`.
The CLI's `supabase db query --file` cannot run these multi-statement scripts
(`cannot insert multiple commands into a prepared statement`), so `psql` is
the documented runner in the new test's header.

**Human gate — passed (branch owner, 2026-09-14), check
`pre-activation-desktop-v2-feed`.** The oldest desktop build that will remain
supported when the reader gate opens, with enhanced attachment emission still
disabled, pulled a complete cloud observation-use feed from a synthetic test
account containing ordinary rows and at least one valid `schema_version` 2
snapshot whose `measurement_details` is top level and whose
`q_core_min`/`q_core_max` are inside `measurements`. The synchronized use and
the other feed rows were then inspected. This is the proof contract section 7
rollout step 1 requires before version-2 emission may ever be enabled: an
older reader neither rejects the whole feed nor silently omits the row.

Sparring returned `READY` on 2026-09-14 for both candidates with no
implementation defect found. Acceptance and merge remain the branch owner's
separate decisions.

Remaining release-owner work, outside this stage: `supabase migration list`,
`supabase db push --dry-run`, `supabase db push` and the post-push
`migration list` check against the deployed project; and any decision to open
either rollout gate, which stay closed here.

## Stage 3C handoff — 2026-09-13 (candidate on `feature/reported-statistics-contract`, sparring pending)

Status: **Stage 3C implemented, self-verified, awaiting sparring.** Cloud
schema/RPC and desktop sync transport of the three extension fields. Stage 3B
(accepted at `ee90fbe0`) and the plan-decomposition commit `d865d91d` are the
base. Nothing Stage 1, 2, 3A or 3B froze was reopened; snapshots, the local
barrier, the parser, editors, plotting and matching are untouched. Merge of
either branch, and deployment of the migration, remain the branch owner's
separate decisions.

- Stage id: `stage-3c-cloud-schema-rpc-and-sync-transport` (brief in
  `.sparring/stages/stage-3c-cloud-schema-rpc-and-sync-transport/`).
- `sporely-py`: branch `feature/reported-statistics-contract`, base
  `d865d91d`; the candidate is the commit that adds this handoff (recorded in
  the stage directory's `handoff.md` and the sparring state).
- `sporely-web`: branch `feature/reported-statistics-cloud-transport`, base
  `d3be3d8a` (`main`), candidate **`b32eb92214f6eae9d308baa17128a53b83b6a896`**,
  pushed. Worked in the linked worktree `sporely-web-reported-statistics`
  because the main checkout carries unrelated uncommitted work on another
  branch; the two SQL files were tested from the main checkout and then
  committed from the worktree (byte-identical), and the untracked copies were
  removed from the main checkout.

Files changed — `sporely-web`:
`supabase/migrations/20260913120000_add_reference_measurement_content_extension.sql`
(new), `supabase/tests/reference_measurement_content_extension_test.sql` (new),
`docs/supabase-sync-contract.md`. Production code changed: yes (migration).
Tests changed: yes. `supabase/schema.sql` not regenerated (AGENTS.md).

Files changed — `sporely-py`: `database/reference_sync_state.py`,
`database/reference_sync_reconciliation.py`, `utils/reference_cloud_adapter.py`,
`utils/cloud_sync.py` (owner read column list only), `docs/supabase-sync-contract.md`;
tests `tests/test_reference_measurement_content_cloud_transport.py` (new, 26
cases), `tests/test_reference_library_pull_reconciliation.py` (`_set_row`
carries the keys), `tests/test_reference_measurement_content_persistence.py`
(two Stage 3B pull expectations, see below), `tests/test_reference_sync_state.py`
and `tests/test_reference_sync_mutation_ownership.py` (baseline shape); this
plan. Production code changed: yes. Tests changed: yes.

Migration and RPC ownership. `20260913120000` adds `measurement_details_json
jsonb`, `q_core_min`, `q_core_max double precision` to
`public.reference_measurement_sets` (nullable, no default, no backfill, no
down step) plus CHECK `reference_measurement_sets_details_shape_check`
(object-or-NULL, `jsonb::text` ≤ 8192 as a loose defence-in-depth bound; the
exact limit lives in the validator). It redefines
`public.sync_reference_measurement_set_unthrottled` (the authoritative
implementation renamed by `20260830193144`; the public
`sync_reference_measurement_set` rate-limit wrapper is untouched) with
`CREATE OR REPLACE`, re-asserting `OWNER TO postgres` and the REVOKEs. New
`private` helpers, all IMMUTABLE and revoked from `PUBLIC, anon,
authenticated`: `reference_jsonb_compact_text` (compact serialization with
byte-ordered keys, the codec's shape for ASCII content),
`reference_positive_finite`, `reference_pair_ordered`,
`reference_json_number_valid`, `reference_range_descriptor_valid`,
`reference_interval_statistic_valid`, `reference_scalar_statistic_valid`,
`reference_measurement_details_valid(jsonb)` (contract §1 structure and
enums) and `reference_measurement_content_valid(public.reference_measurement_sets)`
(contract §2 on the populated record).

Guard and validation rules as implemented (`sync_reference_measurement_set_unthrottled`):

1. Allowlist carries the three keys. Right after it, the count of extension
   keys present must be 0 or 3, else `invalid_payload` (contract §9 item 9),
   before identity parsing and locking.
2. Create branch: if `supersedes_id` names a row with any non-NULL extension
   column and the request omits the keys ⇒ `invalid_payload` (item 4). The
   candidate is populated with `jsonb_populate_record(NULL::row, payload -
   'deleted')` (cast failures ⇒ `invalid_payload`), validated with
   `reference_measurement_content_valid` ⇒ `invalid_payload`, then inserted;
   the INSERT takes the three values from the populated record, so JSON
   `null` and an omitted key both store SQL NULL (item 6). The
   `raw_points_json` insert path is unchanged.
3. Update branch: `v_content_changed` := `to_jsonb(v_next)` and
   `to_jsonb(v_current)` minus `{revision,row_version,created_at,updated_at,
   deleted_at}` are distinct. After the existing first `no_change` return and
   before the CAS check: `v_content_changed AND NOT acknowledging AND
   enhanced(v_current)` ⇒ `invalid_payload` with the current row (item 3).
   The existing same-revision rule now uses `v_content_changed` (same
   projection as before). After the parent, second `no_change` and
   live-use `blocked` checks: `v_content_changed AND NOT
   reference_measurement_content_valid(v_next)` ⇒ `invalid_payload` (item 7),
   so lifecycle-only requests (delete, restore, exact retry) are neither
   guarded nor re-validated and cannot smuggle content (a tombstone that also
   changes a column is content-changed and rejected). UPDATE writes the three
   columns.
4. `reference_measurement_content_valid`: every one of the 15 dimension/Q
   columns NULL or finite and > 0 (NaN and Infinity excluded explicitly);
   the six pairs ordered; then, if details are non-NULL: structure per §1
   (`schema_version` integer; version 1 has exactly `schema_version` and
   `metrics`, non-empty `metrics` over `length|width|q`, non-empty metric
   objects over the five keys, descriptor/interval/scalar shapes and enums,
   percentile bounds `0 <= lo < hi <= 100` only for `percentile_interval`,
   positive interval endpoints with `lower <= upper`, positive median value,
   non-negative sd, booleans are not numbers); compact canonical size ≤ 4096
   bytes; for version 1 the §2 rules 1–4 per metric (outer descriptor ⇒
   complete outer pair, core descriptor ⇒ complete core pair, both ⇒ outer
   encloses core, mean interval ⇒ scalar mean NULL). Any other integer
   `schema_version` is accepted opaquely, bounded by size and by the
   column rules — the stage brief's "unknown future version accepted
   opaquely" (equivalent to `mode="authoritative"`); contract §9 item 7's
   phrase "accepts only schema_version values it knows" is superseded by the
   brief for this stage and should be reconciled in the contract text by the
   reviewer's decision.

Registries extended (contract §4): `_LIBRARY_PAYLOAD_COLUMNS["measurement_set"]`
and `_JSON_PAYLOAD_COLUMNS` (`database/reference_sync_state.py`);
`_PAYLOAD_COLUMNS["measurement_set"]` and `_JSON_COLUMNS`
(`database/reference_sync_reconciliation.py`); `_MEASUREMENT_SET_KEYS`
(`utils/reference_cloud_adapter.py`); the owner read `select=` list of
`SporelyCloudClient.list_reference_measurement_sets` (`utils/cloud_sync.py`).
The adapter's raw-points create workaround is unchanged and explicitly limited
to `raw_points_json`; a NULL extension key is transmitted (test). Every
measurement-set payload therefore carries the three keys, so an unaware
server (C0) rejects every measurement-set write as `invalid_payload` — proven
locally by calling the RPC on the pre-migration local schema
(`PRE-3C OK: enhanced write rejected with invalid_payload`), which is not the
deployed-server proof (human-gated).

Baselines and retries: `recognize_library_baseline(entity_type, payload)`
(`database/reference_sync_state.py`) adds the three keys as `None` to a
measurement-set baseline that carries none of them (a partially carrying
baseline is returned unchanged) and is applied where baselines are read:
`_state_from_row`, `get_library_remote_tombstone`, `list_library_tombstones`,
and `_baseline()` in reconciliation (live and tombstone paths). Incoming
request payloads are never normalized. `_execute_live` is unchanged in code;
tests cover the transitions: unchanged legacy row with a historical baseline
⇒ no RPC call; content edit after upgrade ⇒ one push carrying all keys with
the stored CAS token, baseline replaced; remote tombstone over a historical
baseline ⇒ applied, no conflict; unknown-create recovery matches the remote
row carrying the keys without a duplicate write.

Pull reconciliation (`database/reference_sync_reconciliation.py`):
`stage_reference_library_feed` decodes each remote measurement set's details
and, for enhanced rows only, runs `validate_measurement_content(mode=
"authoritative")` (`_measurement_content_error`); a failure rejects the whole
feed as `ReferencePullReconciliationError` before any write, as the existing
character/data-kind checks do. Legacy remote rows acquire no new rule (test:
a legacy row with an inverted pair still pulls). `_domain_values` stores
`measurement_details_json` through `encode_measurement_details(
decode_measurement_details(...))`, so stored text is the codec's canonical
form. `_reconcile_live`: after computing `local_changes`/`remote_changes`
against the baseline, for `measurement_set` a change set that intersects
`SCIENTIFIC_CONTENT_FIELDS` is expanded to the whole group; conflict iff an
identity field changed remotely or any field in the expanded overlap differs
between local and remote (`overlapping_fields` lists the differing group
fields); merge takes local for every expanded local change, remote
otherwise; the merged measurement set is validated
(`invalid_merged_measurement_content` conflict, no write) before
`_write_domain`. `notes` and identity keep per-field behaviour. The Stage 3B
guard `_extension_write_blocked` and both call sites are unchanged; it fires
only for a payload that does not acknowledge the extension. Since
`canonical_library_payload` now requires the keys, a pre-Stage-3C server's
rows are rejected at staging ("missing canonical fields", zero writes, no
cursor), and the guard is exercised directly in the new test module.

Behaviour changes to flag: (a) two concurrent edits to *different* numeric
fields of one measurement set (for example local `length_max`, remote
`width_max`) now conflict instead of merging — the §5 rule applied to legacy
rows too (test `test_unrelated_numeric_edits_inside_the_group_now_conflict`);
(b) the two Stage 3B pull tests that expected
`unacknowledged_measurement_content_extension` now assert the group-rule
outcome `overlapping_remote_change`, because every row from a supporting
server acknowledges the extension; (c) on the server, any content change to
a *legacy* row is now also validated (finite positive values, ordered pairs),
so a historical cloud row with, say, `length_min = 0` cannot be
content-edited until corrected, while delete/restore still work; (d) an
unaware edit of `notes` alone on an enhanced row is rejected — the server
projection is coarse by design (contract §9 item 3), whereas the desktop
group rule treats `notes` per field; (e) `supabase db reset` from the
worktree applied the migration to the shared local stack, so
`supabase migration list` in the main `sporely-web` checkout shows
`20260913120000` as local-only until the branch is merged.

Pre-existing defect observed, out of scope, not changed: the bare tombstone
payload `{"id", "deleted": true}` that `utils/reference_cloud_sync.py::
_execute_tombstone` sends for measurement sets returns `invalid_parent` from
the unchanged parent check (`taxon_treatment_id` absent ⇒ NULL), on the
pre-3C server as well as now; `reference_library_mutation_test.sql` always
sends the parent id with tombstones, and the new test does the same.
Treatments have the same shape. Recommend a separate stage.

Verification — `sporely-web` (local stack `supabase start`, CLI 2.98.2;
`supabase db query --file` cannot run multi-statement files with this CLI,
so tests ran through `psql` inside the `supabase_db_*` container):
`supabase migration list` before the change — local and remote identical
through `20260831152354`; migration plus new test applied and rolled back in
one transaction against the pre-3C schema (all cases pass); `supabase db
reset --local` from the worktree — all migrations including the new one
applied; then `reference_measurement_content_extension_test`,
`reference_library_mutation_test`, `public_observation_references_test`,
`shared_reference_contributions_test`,
`reference_curated_fork_provenance_test` and
`reference_curation_publication_lifecycle_test` **all PASS** against the
reset database. The new test covers §12 cases 16 and 17: unaware create
without predecessor accepted and returning NULL keys; partial keys rejected
with and without a row; aware create stored exactly and retried as
`no_change`; unaware exact retry `no_change`; unaware bound and notes
mutations `invalid_payload` with the row unchanged; unaware tombstone and
restore `updated` with content intact; tombstone-plus-content rejected;
unaware successor of an enhanced predecessor rejected, aware successor
created, unaware successor of a legacy predecessor created; explicit NULL
clears, then an unaware mutation succeeds; aware re-enhancement; future
`schema_version` 2 stored opaquely; 23 validation rejections on create and
one on update (row unchanged), the clear-then-set-scalar transition
accepted; `reference_canonical_snapshot` of the enhanced row equals its
legacy twin's, is version 1, has no extension key and passes
`reference_snapshot_valid`; validators not executable by `authenticated`/
`anon`; the shape CHECK fires on a raw privileged write. Not run: `npm test`
(no JavaScript changed).

Verification — `sporely-py` (project venv): new module 26 passed; focused
set (`test_reference_measurement_content_cloud_transport`,
`test_reference_library_pull_reconciliation`,
`test_reference_library_push_executor`, `test_reference_cloud_adapter`,
`test_reference_sync_state`, `test_reference_sync_mutation_ownership`,
`test_reference_sync_planner`, `test_reference_cloud_sync_coordinator`,
`test_reference_measurement_content_persistence`,
`test_reference_measurement_content_schema`,
`test_measurement_content_contract_fixtures`,
`test_measurement_content_write_barrier_spike`, `test_measurement_content`,
`test_curated_reference_sync`) **374 passed, 0 failed**; `py_compile` on all
touched Python files ok; `git diff --check` clean. Full suite
(`QT_QPA_PLATFORM=offscreen pytest -q -p no:cacheprovider
--continue-on-collection-errors`, 2 min 30 s): **31 failed, 4251 passed,
10 skipped, 55 errors** against Stage 3B's 31 / 4225 / 10 / 55 — the +26 are
the new module and the failing set is exactly the PROJECT.md baseline
(`qapp` fixture errors in `test_image_gallery_widget` 17,
`test_observations_tab_gallery_move` 12, `test_live_lab_raw_controls` 6;
taxonomy release-directory errors/failures in `test_w2d_reconciliation` 19
errors and `test_supplement_loader`; `test_cloud_media_recovery` collection
error; the listed single failures). No reference-library, sync, import or
archive module regressed.

Human-gated (AGENTS.md), not claimed: deploying `20260913120000`
(`supabase db push` after `migration list` / `db push --dry-run`); live
cloud writes, CAS and cross-client behaviour against the deployed project;
that the *deployed* pre-Stage-3C server rejects an enhanced write (only the
local pre-migration schema was exercised); PostgREST's JSONB rendering of
`measurement_details_json` on a real owner read (the fakes return decoded
objects, which is what PostgREST does).

Deferred: **Stage 3D** — snapshot v2, `reference_canonical_snapshot` v2 for
enhanced rows, curated CHECK/`_validate_snapshot` version awareness,
attachment/export/import transport and the minimum-reader-version gate.
**Stage 4** — editor inspection and guarded editing (cloud support is
dormant: nothing here enables enhanced attachments or editor saving).
Separate follow-up recommended: the bare-tombstone `invalid_parent` defect
above.

Subagents: none used.

## Stage 3B handoff — 2026-09-12 (accepted at `ee90fbe0`)

Status: **Stage 3B accepted** at `ee90fbe08953f42f1f1e1db2f4cf0b56af459d9f`
(the round-1 correction commit; sparring verdict READY). Local persistence of
the typed measurement content through the existing production owners:
repository read/write/successor, bundle import, portable import (merge and
replay check) and pull reconciliation. Stage 3A (schema and barrier, code
candidate `68d1855f`, human gate passed, accepted at `04ea56d7`) is the base;
nothing Stage 1, 2 or 3A froze was reopened. Merge remains the branch owner's
separate decision.

- Stage id: `stage-reported-statistics-local-persistence` (brief in
  `.sparring/stages/stage-reported-statistics-local-persistence/`).
- Branch: `feature/reported-statistics-contract` (linked worktree
  `sporely-py-reported-statistics`). Base SHA: `04ea56d7`.

Files changed: `database/reference_library.py`,
`database/reference_sync_reconciliation.py`, `references/measurement_content.py`,
`utils/db_share.py`, `utils/archive/portable_import.py`; tests
`tests/test_reference_measurement_content_persistence.py` (new, 47 cases),
`tests/test_measurement_content_contract_fixtures.py`,
`tests/test_measurement_content_write_barrier_spike.py`,
`tests/test_reference_library_manager_dialog.py`; this plan. Production code
changed: yes. Tests changed: yes. No schema, cloud, snapshot, parser or UI
change.

Repository (`database/reference_library.py`): `MeasurementSet` gains
`measurement_details_json`, `q_core_min`, `q_core_max` (all default `None`),
the `is_enhanced` property and `measurement_content()` (typed view through
`content_from_row`, contract §3). `MeasurementSetRepository._COLUMNS` lists the
three columns, so `create`, `update` (`allowed` = `_COLUMNS`) and every
`SELECT *` → dataclass read carry them. `_validate` now returns the canonical
`measurement_details_json` text: it builds `content_from_row(asdict(ms))`,
passes the details through `encode_measurement_details` →
`decode_measurement_details` (so semantically empty objects, blank text and
JSON `null` normalize to NULL and stored text is always the codec's canonical
form), then runs `validate_measurement_content(mode="edit")`; a
`MeasurementContentError` becomes `ReferenceValidationError` before any SQL.
`create` and `update` assign the returned text and roll back on any exception;
`create_revision` is unchanged in code (it already copies `asdict(existing)`
and goes through `create`) and therefore copies and re-validates the
extension. Typed → SQLite mapping: numeric columns and the Q core pair map
field-for-field, details map through the codec; SQLite → typed is
`content_from_row`. Edit transitions are the contract's operations applied to
`existing.measurement_content()` with `content_row_updates(...)` passed to
`update`; the repository validates the merged row as state and never repairs
it. Behaviour change to flag: the validator also rejects non-finite,
non-positive and inverted ordinary numeric values that the old `_validate`
accepted; `test_plot_hint_rejects_infinity_and_negative_values` was adjusted
to exercise the hint on unpersisted objects and assert the rejection.

Unsupported future `measurement_details_json` version: read opaquely
(`UnsupportedMeasurementDetails`, stored bytes untouched by reading); the
repository refuses `update`, `create_revision` and `create` of such a row
(`mode="edit"` → "unsupported measurement details version"), so it is
inspect-only until the binary is upgraded, and no successor is created.
Malformed stored text (`{broken`) loads as a row, `measurement_content()`
raises, and any update is rejected; nothing is reinterpreted.
Correction after sparring round 1 (candidate `9d66d6b3`): validating only the
merged candidate let an override of `measurement_details_json` (NULL, or
valid v1 text) through `update` or `create_revision` downgrade a stored
future-version row. `_require_editable_source(existing)` now runs on the
stored row before any override is applied in both paths; the future-version
and malformed-row tests assert that NULL, NULL-plus-cleared-Q-core and v1
replacements are rejected through `update` (with and without revision bump)
and `create_revision`, leaving the raw row byte-identical and no successor.

Import policy (contract §8) is now owned by `references/measurement_content.py`
(`IMPORT_DECISIONS`, `import_decision`, `scientific_content_equal`; moved out
of the Stage 1 fixture test, which now asserts against the production owner).
Bundle import (`utils/db_share.py::_upsert_library_row_by_revision`) applies it
to `reference_measurement_sets` before any SQL, reading acknowledgement from
the source row's key presence before normalization; new no-write outcomes
`rejected_partial_extension`, `rejected_unacknowledged_extension`,
`rejected_invalid_content`, `skipped_unacknowledged`, `conflict` are counted in
`import_database_bundle`'s report (`reference_measurement_set_rejections`) and
appended to `warnings`. Enhanced incoming rows are validated in
`mode="authoritative"` (a future details version is accepted opaquely) and
their details stored in canonical codec form. Behaviour change to flag: at an
equal revision the bundle importer previously returned `skipped_same` without
comparing content; it now compares the §5 scientific-content group
(`scientific_content_equal`, omitted keys read as NULL) and reports
`conflict` when it differs, for legacy rows too. Neither outcome writes
anything, so the only visible difference is the warning. Portable import
(`utils/archive/portable_import.py::_merge_reference_entity`): partial
acknowledgement raises `PortableIdentityConflictError("… incomplete measurement
content extension")` before any other rule, including the insert case; an
omitting source at the same or a higher revision against an enhanced
destination raises `"… source predates measurement content contract"` (no
partial update); enhanced sources are validated (`_prepare_measurement_content`,
`PortableImportError` on failure) before insert and before revision upgrade;
`measurement_details_json` joined `raw_points_json` as a JSON-compared field in
`_merge_reference_graph` and `_validate_replayed_stable_content`, which also
gained the predates check. Legacy sources (no extension keys) keep their exact
previous behaviour and acquire no semantics. Export side audited, unchanged:
bundle and portable export copy the reference database file, so a new exporter
produces complete rows automatically; the v1 observation snapshot projection of
an enhanced row equals that of the same row stripped of the extension (test).
Curated fork copy (`copy_curated_bundle_to_personal_library`) reads v1
snapshots, which cannot carry the extension, and produces legacy-only rows
(§8); unchanged, v2 is Stage 3D.

Pull reconciliation (`database/reference_sync_reconciliation.py`): remote
payloads do not acknowledge the extension before Stage 3C, and `_write_domain`
upserts only `_PAYLOAD_COLUMNS`, so writing a remote change over a locally
enhanced row would leave descriptors describing numbers they were not written
for. `_extension_write_blocked` (kind `measurement_set`, remote does not
acknowledge, stored local row enhanced) now turns both `_reconcile_live` write
sites (remote-only change, and merge) into a recorded conflict with reason
`unacknowledged_measurement_content_extension` and no domain write. New-row
adoption (`local is None`) and legacy rows are untouched. The §5 group rule for
cloud reconciliation is Stage 3C work, when remote rows carry the group.
Deliberately not extended: `_LIBRARY_PAYLOAD_COLUMNS`, `_PAYLOAD_COLUMNS`,
`_JSON_COLUMNS`, `_MEASUREMENT_SET_KEYS` — pushing the keys before the cloud
allowlist accepts them would reject whole feeds (§4).

Barrier: no new registration. Repository connections come from
`_connect_reference` / `get_reference_connection`, bundle import from
`get_reference_connection`, portable import registers after `ATTACH` — all
Stage 3A sites. Tests prove repository INSERT, UPDATE and successor of enhanced
rows succeed, that raw unaware INSERT/UPDATE stays blocked after repository
use (also on legacy rows), and that a fresh `sqlite3.connect` has no
`sporely_measurement_contract` function (registration is per connection).

Atomicity: validation precedes every statement; `update` writes all allowed
columns (ordinary and extension) in one UPDATE plus intent recording, commits
once and rolls back on any exception. Regression: a failure injected into
`record_library_mutation_intent` after the UPDATE leaves the raw row
byte-identical; a failed validation of a combined ordinary+details update
leaves it byte-identical; every rejected import outcome writes nothing
(asserted with before/after raw rows).

Parser boundary: `ParsedMeasurement.to_content()` (Stage 2) is the typed
output, but no production path saves parser output directly — both editors
parse into table cells and build a `MeasurementSet` from the cells
(`ui/reference_entry_editor.py::_build_measurement_set`,
`ui/reference_library_manager_dialog.py::_on_save`). Wiring the typed output
through those editors is editor work (Stage 4). No parser change.

Verification (project venv): new module 47 passed; with the fixtures, spike,
content, schema and manager-dialog modules 248 passed; reference-library /
curated / legacy / reference-use / portable / full-backup / db-share / archive
/ measurement regressions **1 failed, 1036 passed** — the one failure is the
pre-existing `test_archive_inventory` table-set assertion (PROJECT.md item 4);
`py_compile` on all nine touched Python files ok; `git diff --check` clean.
Full suite (`QT_QPA_PLATFORM=offscreen pytest -q -p no:cacheprovider
--continue-on-collection-errors`, 2 min 32 s): **31 failed, 4225 passed,
10 skipped, 55 errors** against the baseline 31 / 4178 / 10 / 55 — the +47
are the new module and the failing set is exactly the PROJECT.md baseline
(`qapp` fixture errors, taxonomy release-dir errors/failures,
`test_cloud_media_recovery` collection error, and the listed single
failures). No reference-library, archive, import or sync module regressed.
After the round-1 correction: persistence module 47 passed; the
reference-library / curated / legacy / reference-use / sync / editor /
fixture / spike / content / schema set 519 passed; `py_compile` and
`git diff --check` clean. The correction touches only `update` and
`create_revision` guards plus their tests, so the full suite was not rerun.

Deferred: **Stage 3C** — cloud columns/RPC allowlist, payload and key
registries (§4), remote acknowledgement, §5 group rule in `_reconcile_live`
replacing the fail-closed guard, cloud transport of the extension. **Stage 3D**
— snapshot v2, curated copy of v2 snapshots, attachment/export/import
representation and minimum-reader-version gating. **Stage 4** — editor
inspection and guarded editing, `to_content()` → repository wiring.

Subagents: none used.

## Stage 3A handoff — 2026-09-12 (candidate `68d1855f`, human gate passed)

Status: **Stage 3A candidate READY (sparring verdict on `68d1855f`) and the
human v0.9.22 compatibility gate passed** (evidence recorded below, in the
commit that carries this paragraph). Local SQLite schema extension and
old-client write barrier only. Stage 2 is accepted at
`829be09b9298ecf4e8423bb7278301af876b85d1`; nothing Stage 1 or Stage 2
froze was reopened. Frozen code candidate: `68d1855f37dd64a2e29322ed5ab4a3b0ff36cce2`
(one correction commit on top of the sent-back `713c6b4b`; this
evidence-recording commit is documentation only). Acceptance and merge remain
the branch owner's separate decision.

- Stage id: `stage-reported-statistics-local-schema-barrier` (brief in
  `.sparring/stages/stage-reported-statistics-local-schema-barrier/`).
- Branch: `feature/reported-statistics-contract` (linked worktree
  `sporely-py-reported-statistics`). Base SHA: `829be09b`.
- First candidate `713c6b4b` received a sparring SEND_BACK (migration
  atomicity gap, below). The correction is a new commit on top; the current
  candidate SHA is the commit that carries this section (also recorded in the
  stage notes once known).

Sparring round 1 (SEND_BACK on `713c6b4b`) and response: fresh table
creation (DDL with the extension columns) and the CASCADE-era rebuild both
committed the extended table before the guard triggers existed, because
Python's `sqlite3` autocommits DDL and the rebuild commits its own
transaction; an interruption there left committed columns without a barrier.
Response: (1) `init_reference_library_schema` now opens an explicit
transaction (unless the caller holds one) around the core table DDL, the
extension step and the indexes, and rolls it back on any failure; the
extension step runs immediately after the sets DDL inside that transaction.
(2) `_rebuild_table_with_restrict_fks` gained `post_ddl` statements executed
on the rebuilt table before its commit; `_ensure_restrict_foreign_keys`
passes the guard-trigger DDL for `reference_measurement_sets`, so a rebuild
never commits without the guards. (3) New tests inject a failure at every
statement position of initialization (fail-on-Nth-statement connection
factory) for empty, legacy and CASCADE-era start states, and after each
failure assert through an unregistered connection that extension columns are
never present without both triggers and a rejected UPDATE; then resume and
assert convergence to the steady state. Plus a direct rebuild test with a
failing `post_ddl` that rolls the whole rebuild back. Observed while doing
this, pre-existing and left as is: a CASCADE-era rebuild drops the two
`reference_measurement_sets` indexes with the old table and the same run does
not recreate them; the next start does.

Schema (production owner `database/reference_library_schema.py`):

- `_REFERENCE_MEASUREMENT_SETS_DDL` now declares `measurement_details_json
  TEXT`, `q_core_min REAL`, `q_core_max REAL` as the last three columns
  (nullable, no default), so fresh and upgraded libraries have the same
  column order.
- `_ensure_measurement_content_extension(conn)` adds any missing extension
  column with `ALTER TABLE … ADD COLUMN` and creates the two guard triggers
  (`IF NOT EXISTS`); it runs inside the explicit transaction that
  `init_reference_library_schema` opens around table creation, so no
  committed state has the columns without the barrier. The CASCADE→RESTRICT
  rebuild, which drops triggers, recreates the guards inside its own
  transaction (`post_ddl`). A no-op when both columns and triggers exist:
  repeated initialization leaves `sqlite_master`, `PRAGMA data_version` and
  every row unchanged. Nothing reads, validates or rewrites
  `measurement_details_json`; legacy rows get NULL in all three fields and no
  invented semantics.

Barrier mechanism (exactly contract §6 / spec §14): triggers
`reference_measurement_content_guard_update` (BEFORE UPDATE, WHEN old or new
row enhanced) and `reference_measurement_content_guard_insert` (BEFORE INSERT,
WHEN new row enhanced or its `supersedes_id` points at an enhanced row), body
`RAISE(ABORT, 'measurement content contract required')` when
`coalesce(sporely_measurement_contract(), 0) < 1`. Because SQLite resolves
functions at prepare time, a connection without the function cannot compile
*any* INSERT or UPDATE on the table (`OperationalError: no such function:
sporely_measurement_contract`), including on legacy rows; this is the coarse
unsupported-open policy accepted 2026-09-11. DELETE and SELECT are outside the
barrier and unchanged (the existing tombstone trigger still fires).

How supported code identifies itself: `register_measurement_contract(conn,
version=1)` (schema owner) calls `conn.create_function` for
`sporely_measurement_contract`. It is invoked (a) as the first statement of
`init_reference_library_schema`, so `_connect_reference`, the repository,
pull reconciliation, curated copy, bundle import and `init_reference_database`
are covered; (b) in `database/schema.py::get_reference_connection`; (c) in
`utils/archive/portable_import.py::import_portable_payload` right after
`ATTACH … AS portable_reference` (the one production writer that neither
calls init nor uses the factory). No import/export *behavior* changed; those
two lines only keep the new application from blocking itself. Awareness is
connection-scoped and cannot leak: an aware writer committing does not open
the door for an unaware connection.

Old-client operations on an upgraded library: rejected — INSERT (repository
shape, `INSERT OR REPLACE`, `REPLACE`, `INSERT … ON CONFLICT DO UPDATE`,
`INSERT OR IGNORE`, successor of an enhanced row), UPDATE (content edit,
notes-only, revision bump, legacy-id enrichment, `UPDATE OR IGNORE`),
through an ATTACHed database, inside `executescript`, and inside an explicit
transaction (rollback restores everything, including a DELETE made earlier in
the same transaction). Allowed — SELECT, DELETE, writes to every other table,
the older binary's startup statements (table/index/trigger `IF NOT EXISTS`,
sync-state backfill).

Tests: `tests/test_reference_measurement_content_schema.py` (new, 43 cases)
covers brief items 1–16 and the interruption invariant: populated legacy upgrade (RESTRICT- and CASCADE-era
DDL built from the pre-feature table definition), fresh library through
`init_reference_database`, exact `PRAGMA table_info` for the three columns,
identical shape fresh vs upgraded, legacy rows NULL/not enhanced with every
old value byte-identical, idempotence (three runs, `data_version`, integrity
check, no duplicate columns), enhanced values and malformed/future/blank
details text untouched by re-initialization, columns and triggers restored
together, factory/repository/init-registered/explicit writers pass,
version-0 registration stopped by the trigger body, and the old-client matrix
above using raw SQL shaped like the pre-feature `MeasurementSetRepository`
(its 31-column list, statement forms and connection pragmas). Item 17:
`tests/test_measurement_content_write_barrier_spike.py` no longer carries its
own DDL/helper; its 20 Stage 1 scenarios (importer merge paths, upsert,
successor, table rebuild, contract-spec constants) now run against the
production barrier, and the "older startup" case executes the pre-feature
init statements on an unregistered connection. Eight existing test modules
that seed measurement rows through raw `sqlite3.connect` now call
`register_measurement_contract` at those seeding sites (the contract's
"tests and tools writing rows directly" row); no assertion changed.

Verification (project venv, after the correction): new module 43 passed; with
spike + fixtures + content modules 172 passed; reference-library / curated /
legacy / reference-use / portable / backup / db-share / archive regressions
860 passed with the one
pre-existing `test_archive_inventory` failure (table-set assertion about
`observation_reference_use_cloud_*` tables, PROJECT.md item 4, unrelated to
the new columns); `py_compile` on all 12 touched files ok; `git diff --check`
clean. Full suite (`--continue-on-collection-errors`) on the corrected
code: **31 failed, 4178 passed, 10 skipped, 55 errors, exit 1** (first
candidate 31 / 4174 / 10 / 55; Stage 2 31 / 4135 / 10 / 55). The failing and
erroring module set is exactly PROJECT.md items 1–4 (`test_w2d_reconciliation`
19 E + 1 F, `test_image_gallery_widget` 17 E + 1 F, `test_supplement_loader`
12 F, `test_observations_tab_gallery_move` 12 E,
`test_observation_geography_sync` 9 F, `test_live_lab_raw_controls` 6 E,
`test_render_review_screenshots` 4 F, `test_taxon_lookup`,
`test_sample_source_ui_presence`, `test_archive_inventory`, `test_ai_id_parity`
1 F each, `test_cloud_media_recovery` 1 collection E); nothing else fails.

Deferred to Stage 3B by design: `MeasurementSet` / `_COLUMNS`, payload and key
registries (contract §4), `_validate` → `validate_measurement_content`,
`create_revision` copying the extension, import decisions (§8), reconciliation
JSON canonicalization, snapshot v2, cloud allowlist, editors. The repository
still reads and writes only the pre-feature columns; it passes the barrier
because its connection is registered.

**Human gate — passed (branch owner, 2026-09-12).** The actually shipped
macOS Sporely v0.9.22 was run against an isolated copy under
`~/Desktop/sporely-old-client-test` that candidate `68d1855f` had upgraded.
The pre-Stage-3A baseline had established that v0.9.22 could read, update
and insert reference measurements in that copy. After the upgrade: v0.9.22
opened and read the reference library; UPDATE of an existing reference
measurement was blocked; INSERT/create of a new reference measurement was
blocked; the failure was the old connection lacking the measurement-contract
registration (the accepted mechanism); SQLite `quick_check` remained `ok`;
before/after semantic snapshots of `reference_measurement_sets` were
identical; no partial mutation was observed. This is the shipped-old-build
verification the contract (§6) lists under `release_gates`. DELETE was not
reported as exercised; its policy is covered by the automated tests.

Subagents: none used.

## Stage 2 handoff — 2026-09-12 (accepted at `829be09b`)

Status: **Stage 2 candidate ready for independent review** (typed contract
module and parser specification; pure Python and tests only). No schema,
cloud, sync, snapshot, UI or persistence change. Stage 1 remains accepted at
`a0bdcd3737370b307717a71e6ff2579798604ee4`; nothing it froze was reopened.

- Stage id: `stage-reported-statistics-typed-parser` (brief in
  `.sparring/stages/stage-reported-statistics-typed-parser/`).
- Branch: `feature/reported-statistics-contract` (linked worktree
  `sporely-py-reported-statistics`). Base SHA: `a0bdcd37`.
- Candidate SHA: recorded in the stage notes and the commit that carries this
  section (the section is committed together with the code). The first
  candidate `7e64508a` and the second `f8c4505d` each received a sparring
  SEND_BACK; every correction is a new commit on top (no history rewritten).

Sparring round 2 (SEND_BACK on `f8c4505d`; finding 4 still reproducible for
malformed intervals and numeric suffixes) and response, in the third
candidate commit: the named-value regex could backtrack to a shorter prefix
(`Qav = 1.5-2e2` → `1.5`, leaving `-2e2` in the dimension remainder).
`_strip_named_values` no longer uses an optional regex group. A named value's
expression now runs from its `=` to the next `,`/`;`, the next named label,
or the end of the string, and is accepted only when that whole expression
`fullmatch`es one value token (`_VALUE_TOKEN_FULL_RE`); otherwise the complete
expression is rejected with `"<label>: could not parse '<expression>'."`.
Accepted or rejected, the expression is removed from the remainder, so no
suffix reaches width parsing. Consequence, deliberately accepted: a stray
token after a value (`Qm = 1.5-1.7 x`) or a named value glued to the
dimensions without a separator (`Qm = 1.9 x 5-6`) is now rejected as a whole
rather than partially read. Regressions: `1.5-2e2`, `1.5-`, `1.5-2.5%`,
`1.5±0.2`, `1.5/2`, `1.5 approx`, `1.5-1.7 x`, `(1.3) 1.4-1.9 (2.1)x`,
`1.5 1.7`, `-1.5`, each asserting no Q mean, the exact warning and an intact
width; valid separations (`Q = 1.2-1.4 Qm = 1.3 n = 30`, comma-separated
Hebeloma strings) keep parsing.

Sparring round 1 (SEND_BACK on `7e64508a`) and responses, all in the second
candidate commit:

1. *Heading recognition could invent semantics and shift columns.* Heading
   words now match only as whole cells (delimited) or whole words
   (space-separated): `mean error` or `meaningful` is an unknown heading, not
   a mean column (`_classify_heading_cell`). In a space-separated heading,
   unrecognised text keeps its position as one unknown column and marks the
   heading ambiguous (`_tokenize_heading`); rows under it bind only when their
   value count equals the column count. The glued Hebeloma form stays an
   explicit pattern, now for any `N%-M%` percentiles. Delimited rows bind by
   full column position including the row-label cell, so a header without a
   label column, or with an unknown first heading, still aligns.
2. *Short space-separated rows shifted statistics.* A space-separated row
   whose value count differs from the heading's column count binds nothing
   and warns (`"… with no cell boundaries; row not read"`); only tab/pipe
   rows, which have explicit boundaries, keep positional binding of short or
   long rows with per-column warnings.
3. *Non-positive dimensions and scalar means passed validation.*
   `_validate_columns` now requires every dimension, core, Q and scalar-mean
   column to be a finite, non-boolean, **positive** number in both modes;
   positivity no longer depends on a descriptor. The test that accepted a
   negative untagged bound was replaced by parametrised rejections of
   negative/zero/NaN bounds, scalar means and untagged pairs.
4. *Numeric prefixes leaked into width.* First response (a negative lookahead
   after the token) was incomplete because the regex could still backtrack to
   a shorter prefix; superseded by the round 2 response above.

Deliverables:

- `references/measurement_content.py` (new): the shared contract module with
  exactly the public surface of contract §3 — constants (`METRICS`,
  `EXTENSION_FIELDS`, `SCIENTIFIC_CONTENT_FIELDS`, the three kind sets,
  `MEASUREMENT_DETAILS_MAX_BYTES`, `SUPPORTED_DETAILS_VERSIONS`),
  `MeasurementContentError`, the frozen dataclasses (`RangeDescriptor`,
  `IntervalStatistic`, `ScalarStatistic`, `MetricDetails`,
  `MeasurementDetails`, `UnsupportedMeasurementDetails`, `MeasurementContent`),
  the codec (`decode_measurement_details`, `encode_measurement_details`,
  `measurement_details_equal`, plus `details_to_object` for the decoded
  object snapshot v2 will embed), row helpers (`content_from_row`,
  `content_row_updates`, `is_enhanced_row`, `acknowledgement_state`,
  `acknowledges_extension`), `validate_measurement_content(content, *, mode)`
  and the edit operations `clear_pair`, `clear_statistic`, `set_scalar_mean`,
  `set_mean_interval`, `swap_length_width`. `MeasurementContent` holds the 26
  group fields with `measurement_details_json` represented decoded as
  `details`. Decode raises on malformed JSON or wrong *shape*; semantic rules
  (enums, ordering, signs, emptiness, the 4096-byte limit) live in validate,
  so hand-built dataclasses are checked the same way as decoded text.
  Validation modes follow §3: `edit` rejects unsupported versions,
  `authoritative` accepts them opaquely and still enforces finite positive
  column values, pair ordering and size. Edit operations are pure transitions that never
  validate the whole row; all of them refuse `UnsupportedMeasurementDetails`.
- `references/measurement_parser.py` (extended, same public names): HTML
  entity decoding and `<br>` normalisation before any splitting (`<br>` is a
  row break, but a soft space inside a Markdown row); table recognition on the
  structure-preserving text before whitespace folding (`_parse_table`,
  `_classify_line`, `_parse_header`, `_apply_row`); heading-driven column
  mapping for tab, pipe and space delimited rows with per-column binding (a
  missing or malformed cell never shifts a neighbour; unknown, duplicate,
  ambiguous headings and malformed/short/extra cells each warn by name); the
  glued `Spore(min) 5%-95% (max)meanmedianS.D.` heading as an explicit form
  (`_GLUED_HEBELOMA_HEADING_RE`); unlabelled rows under a heading assumed
  L/W/Q only when there are two or three, with a warning; headerless labelled
  rows read with the documented layout (range, or range/mean/median/S.D.) and
  core tagged `unspecified`; `Qav`/`Qm` aliases (scalar → `q_mean`, interval →
  Q `mean_interval`), ordinary `Q =` ranges independent, repeated named values
  with different numbers warn and keep the first, a single Q value together
  with Qm warns and keeps both; a named value's whole expression up to the
  next separator or label must be exactly one numeric token, so it can neither
  swallow a following dimension nor leak a suffix into it. New result fields
  `length_mean`, `width_mean`, `metric_details: dict[str, MetricDetails]` and
  `MeasurementParseResult.to_content()`; `swap_length_width` moves numbers,
  scalar means and details together. Range descriptors emitted by the parser:
  `percentile_interval` only from an explicit `N%-M%` heading, otherwise
  `unspecified` for every inner pair; `reported_extremes` only when both
  parenthesised extremes are present. The legacy `p50` centre (single value or
  `a-b-c` form) has no typed home and stays a legacy-editor value; the
  contract carries no untyped scalar and the plan forbids inferring a mean.
- Tests: `tests/test_measurement_content.py` (new; 81 collected cases incl.
  22 mutation ids: contract §12 cases 1, 2, 3 and 18 at module level,
  spec-block linkage, codec, both modes, positivity, every edit operation)
  and Stage 2 cases appended to `tests/test_measurement_parser.py` (96
  collected cases, 69 new; the existing 27 unchanged). The Stage 1 fixture
  `row_enhanced.json` is
  reproduced end to end: parsing its `raw_text` yields its 15 numeric columns
  and encodes to its stored `measurement_details_json` byte for byte.

Existing consumers: `DimensionRange`, `q_mean`, `n`, `to_record_dict()` and
the warning strings the entry editor filters are unchanged; both editors
(`ui/reference_entry_editor.py::_set_parsed_result`,
`ui/reference_library_manager_dialog.py::_on_parse_clicked`) read only those
and were not modified. Intervals, medians and S.D. reach neither `p50` nor
`q_mean`; only a scalar *mean* cell or scalar Qm/Qav fills a scalar mean.

Verification (project venv; commands per `.sparring/PROJECT.md`): new/extended
modules `test_measurement_content` + `test_measurement_parser` 177 passed;
with the Stage 1 fixture and barrier-spike modules, both editor test modules
and `test_reference_library_{schema,repository}` 329 passed;
reference-library regressions
(`test_reference_library_{schema,repository,snapshot,pull_reconciliation,bundle_roundtrip,manager_dialog}`,
`test_curated_reference_forks`, `test_legacy_reference_migration`,
`test_reference_add_dialog_normalized`) 181 passed; `py_compile` on the four
touched files ok; `git diff --check` clean. Full suite
(`--continue-on-collection-errors`, as for Stage 1): **31 failed,
4135 passed, 10 skipped, 55 errors, exit 1** (earlier candidates:
31 / 4100 / 10 / 55 and 31 / 4125 / 10 / 55) against the Stage 1 report of
31 failed / 3983 passed / 10 skipped / 55 errors. The failing and erroring
module set is identical to PROJECT.md items 1–4 (`test_w2d_reconciliation`
19 E + 1 F, `test_image_gallery_widget` 17 E + 1 F, `test_supplement_loader`
12 F, `test_observations_tab_gallery_move` 12 E,
`test_observation_geography_sync` 9 F, `test_live_lab_raw_controls` 6 E,
`test_render_review_screenshots` 4 F, `test_taxon_lookup`,
`test_sample_source_ui_presence`, `test_archive_inventory`, `test_ai_id_parity`
1 F each, `test_cloud_media_recovery` 1 collection E). No Supabase
connection, no schema or cloud operation.

Decisions the reviewer should challenge (recorded so they are not silent):

1. The parser tags the core pair `unspecified` for every inner range without
   a percentile heading, including the compact `A-B x C-D` form. This is the
   plan's "marked with unspecified meanings" and contract §1's "statement by
   the parser"; the alternative (no tag) would make parser output
   indistinguishable from never-examined legacy rows.
2. The parser tags `reported_extremes` when both parenthesised extremes are
   present, in tables and compact strings alike. Parentheses are the notation
   the module docstring has always read as extreme observations; a one-sided
   extreme leaves the pair untagged because rule 1 of §2 needs a complete pair.
3. A scalar Q *mean* cell in a headed table fills `q_mean`, exactly like
   `Qm = x` already does. Separating generic Q means from the Parmasto widget
   is Stage 4 UI work (plan: *Parser and UI behavior*).
4. `decode_measurement_details` raises on shape errors (non-object, unknown
   keys, wrong key sets) while `validate_measurement_content` owns semantics.
   Both raise `MeasurementContentError`.

Not done, by design (Stage 3 or later): no column, migration, trigger,
`register_measurement_contract`, payload-registry, reconciliation, snapshot,
importer, cloud or UI change; `MeasurementSetRepository._validate` does not
yet call `validate_measurement_content`; nothing consumes `to_content()`.
`tests/test_measurement_content_contract_fixtures.py` keeps its restated
helpers (import decision and snapshot projection have no production owner
until Stage 3); the new module tests the real code against the same fixtures.

Repository hazard to note: the canonical checkout
(`sporely-py`, branch `feature/reference-save-and-plot`) still carries
*uncommitted*, human-gated parser work (`SummaryStatistics`, `_parse_table`,
`range_semantics`) recorded in `2026-09-09-reference-measurement-table-parser.md`.
This stage supersedes that scope on this branch (same inputs, typed output);
the two must not both land. That local work was read for consistency and not
touched.

Subagents: none used.

## Stage 1 handoff — 2026-09-11 (accepted at `a0bdcd37`)

Status: **Stage 1 candidate ready for independent review** (contract frozen,
fixtures and barrier spike committed green). No production code, schema, UI or
parser change. `2026-09-09-reference-measurement-table-parser.md` retains the
existing parser-stage verification record.

Human review decisions (2026-09-11), recorded in the commit that carries this
paragraph, on top of frozen candidate `3f8066c` (sparring verdict READY after
two send-backs): the coarse unsupported-open policy is accepted as final;
enhanced-bundle exposure uses a minimum-supported-desktop-version policy, not
release notes; enhanced attachments use a minimum-supported-reader-version
gate, not a waiting period; shipped-old-build verification stays human-gated
and is a prerequisite for the persistent local-schema/barrier sub-stage.
Changes: contract §6, §7 step 5, §11, §12 cases 19–20, §13, §14 spec
(`release_gates`, `unsupported_open_policy`); one new contract-consistency
test; this handoff. No production code; no settled schema/ontology choice
reopened. A new candidate SHA supersedes `3f8066c` for acceptance.

- Stage id: `stage-reported-statistics-contract` (brief in
  `.sparring/stages/stage-reported-statistics-contract/`).
- Branch: `feature/reported-statistics-contract` (linked worktree
  `sporely-py-reported-statistics`).
- Base SHA: `8097bc8f9689a730b1cfb9991ff3874eae3d2930`. The plan/sparring
  configuration commit `09afc99` sits between base and candidate.
- Content commits: `b98da04d` (fixtures, contract-consistency tests,
  write-barrier spike), `7a2e1e84` (contract document), `865654be`
  (old-client matrix correction), `1b098a43` (response to the first
  sparring round) and `a0f12f3f` (response to the second round, both below).
  **Frozen content SHA: `a0f12f3f03234dc40fa1157c86887b27ac2060bb`.**
  The commit carrying this handoff follows it and changes only this plan file.
- Deliverables: `docs/reference-data/measurement-content-contract.md`
  (684 lines; over the ~600 guideline because the sparring rounds added the
  partial-acknowledgement state, validation modes, a corrected matrix and the
  machine-readable spec block — trimmed where possible, content kept);
  `tests/fixtures/reference_statistics/*.json` (9 fixtures);
  `tests/test_measurement_content_contract_fixtures.py` (28 tests);
  `tests/test_measurement_content_write_barrier_spike.py` (19 tests).

Verification at `a0f12f3f` (project venv, commands per `.sparring/PROJECT.md`):
new modules 47 passed; required unaffected modules unchanged —
`test_reference_library_schema` 13, `_repository` 23, `_snapshot` 9,
`_pull_reconciliation` 17, `_bundle_roundtrip` 6 (115 passed together);
`py_compile` on both new test modules ok; `git diff --check` clean. Full
suite: the plain command aborts at collection on the pre-existing
`tests/test_cloud_media_recovery.py` shadowing error (baseline item 3), so it
was rerun with `--continue-on-collection-errors`: **31 failed, 3983 passed,
10 skipped, 55 errors, exit 1** (2 min 25 s) against the baseline
31 failed / 3936 passed / 10 skipped / 55 errors. The +47 passes are exactly
the new tests; the failing and erroring module set is identical to PROJECT.md
items 1–4 (`test_w2d_reconciliation` 19 E + 1 F, `test_image_gallery_widget`
17 E + 1 F, `test_supplement_loader` 12 F, `test_observations_tab_gallery_move`
12 E, `test_observation_geography_sync` 9 F, `test_live_lab_raw_controls` 6 E,
`test_render_review_screenshots` 4 F, `test_taxon_lookup`,
`test_sample_source_ui_presence`, `test_archive_inventory`, `test_ai_id_parity`
1 F each, `test_cloud_media_recovery` 1 collection E). No Supabase connection,
no schema or cloud operation.

Second sparring round (SEND_BACK on `1b098a43`) and responses in `a0f12f3f`:
(1) handoff, counts and push completed here; (2) curated copy is now stated
as the one authoritative path that rejects unsupported details, at
`copy_curated_bundle_to_personal_library`, and removed from the opaque-accept
list in §3; (3) the §11 pull row is split into never-upgraded libraries
(degraded editable copy) and upgraded libraries (fail closed at
`_write_domain`, transaction rolled back, no partial write).

First sparring round (SEND_BACK on `865654be`) and responses, all in `1b098a43`:
(1) partial acknowledgement is now a third state, rejected on every path, with
fixture `import_row_partial_extension.json` and tests; (2) matrix §11 now
states that never-upgraded libraries give older desktops no local protection
and that pulled copies are editable until the server rejects the push; C1 is
defined as columns plus guard live, accepting enhanced writes; (3)
`validate_measurement_content` gained a required `mode` (`edit` /
`authoritative`) with the opaque-future-version rules, and server validation is
row-level cross-field, not JSON-only; (4) the contract embeds a
`json contract-spec` block that both test modules parse and assert against;
(5) this handoff, counts and push complete the review surface.

Blocker disposition (details and symbol citations in the contract document):

1. **Conflict group — resolved.** 26 explicit fields (contract §5); identity
   fields stay under `_IDENTITY_FIELDS`; `notes` explicitly excluded; group
   moves as one unit in `_reconcile_live`, then the merged row is validated.
2. **Local write barrier — resolved by executable spike, with one discovered
   property and one open item.** Trigger pair calling a connection-registered
   SQL function; unaware connections fail at statement prepare. Proven for
   content edit, clearing the extension, attached-database import
   (`_merge_reference_entity`), bundle merge (`_upsert_library_row_by_revision`),
   pull-style upsert, unaware successor insert, and old-binary table rebuild.
   Discovered: function resolution precedes the trigger `WHEN` clause, so an
   unaware binary can neither INSERT nor UPDATE any `reference_measurement_sets`
   row once the barrier exists (reads, deletes and every other table work).
   This is the unsupported-open policy, **accepted by human review on
   2026-09-11** as final: no finer row-dependent compatibility for unaware
   binaries. Human-gated and a **prerequisite for the persistent
   local-schema/barrier sub-stage of Stage 3**: running an actual shipped
   older build against an enhanced library. An older binary importing an
   enhanced bundle into a library that no aware binary ever opened is lossy
   and cannot be stopped by schema; the accepted mitigation is a
   minimum-supported-desktop-version policy (release notes alone are not a
   safety mechanism), and every supported import path must preserve enhanced
   content or reject it explicitly.
3. **Snapshot v2 — resolved.** Exact shape (details outside `measurements`,
   `q_core_*` inside, 65536/4096-byte limits), version-aware projection rule,
   emit-v1-for-legacy rule, and a five-step reader-first rollout. Decided by
   human review on 2026-09-11: activation of enhanced attachments and v2
   emission sits behind a **minimum-supported-reader-version gate**; they stay
   disabled until every supported desktop version can consume v2 safely,
   since pre-reader desktops reject the whole use feed. No waiting period.
4. **Import omission/NULL/revision policy — resolved.** Key presence decides
   acknowledgement (`absent` / `complete` / `partial`) before any
   normalization; partial rows are rejected everywhere; omitting
   higher-revision rows against an enhanced destination are rejected
   explicitly; explicit NULL clears; omission equals NULL only against
   non-enhanced destinations. The spike records today's lossy-upgrade
   behaviour as evidence.

Also frozen: shared module `references/measurement_content.py` and its API
(with validation modes), single codec (`json.dumps(..., ensure_ascii=True,
sort_keys=True, separators=(",", ":"))`) with decoded-object equality, cloud
key-presence rules using the existing `invalid_payload` status with
tombstone/lifecycle exemptions and row-level server validation, writer/reader
map, old-client matrix, and 18 enumerated Stage 2/3 acceptance cases (no
skipped or xfail placeholders written).

Plan amendments made by Stage 1 (contract §13): the *Local/offline
compatibility* paragraph below now records the proven barrier and its coarse
effect; `notes` is outside the conflict group; rejected unaware cloud
mutations reuse `invalid_payload`; partial acknowledgement is a third state.

Deferred to later stages: everything in *Revised implementation sequence*
items 2–5; the cloud sub-stage in `sporely-web` (validators, columns, RPC
guard, canonical/public snapshot builders, curated CHECK relaxation).

Subagents: none used.

The earlier design-revision handoff (2026-09-11, no implementation) remains
summarized in the sections below; its probe findings are now superseded by the
executable evidence cited in the contract document.

## Problem and user intent

Literature gives several distinct kinds of information:

- Ordinary spore measurement ranges, sometimes with parenthesized extremes.
- Inner ranges described informally as typical, or explicitly as percentiles.
- Scalar means and ranges of reported means, for length, width, and Q.
- Median ranges and reported SD with potentially unspecified population/method.
- Formal statistics, including Parmasto statistics, when actually supplied.

The user wants numeric values accompanied by meaning tags. A typical range must
not become a 5th–95th-percentile interval merely because legacy database columns
are called p05/p95. A mean interval must not be converted to a scalar midpoint,
ordinary spore bounds, CV, or a formal statistical interval.

Example source header: `Spore(min) 5%-95% (max)meanmedianS.D.`.
Length: `(6.9) 8.0–15.2 (16.1)`, mean `8.9–13.7`, median `9.0–13.9`, SD `0.696`.
Width: `(4.1) 5.1–8.2 (8.9)`, mean `5.6–7.5`, median `5.6–7.5`, SD `0.323`.
Q: `(1.17) 1.36–2.19 (2.79)`, mean `1.51–1.96`, median `1.50–1.95`, SD `0.097`.
The heading establishes the inner percentiles. It does not establish whether the
mean ranges are extrema across specimens, confidence intervals, or something else.

Also handle ordinary strings `8.5-12.5 x 4.5-5.5, Q = 1.7-2.5` and
`7-9 x 4.5-5.5 Qav = 2.3-3;`, including literal HTML whitespace entities.

## Observed implementation and reuse targets

- `references/measurement_parser.py`: `SummaryStatistics` already captures
  mean/median tuples and SD; `MeasurementParseResult.range_semantics` is currently
  one global marker. `_parse_table` assumes range/mean/median/SD ordering, accepts
  range-only rows, and rejects partial/reordered summary columns. `to_record_dict`
  omits the extra statistics. Reuse and extend this parser, not a second parser.
- `database/reference_library.py::MeasurementSet` has scalar means and L/W core
  bounds, but no structured reported statistics or Q core bounds.
  `MeasurementSetRepository._COLUMNS` and `_validate` own persistence/validation.
- `database/reference_library_schema.py::init_reference_library_schema` owns
  normalized local schema initialization; use its established migration mechanism.
  `database/sqlite_migrations/README.md` confirms Postgres migrations do not belong
  in the SQLite migration directory.
- `ui/reference_entry_editor.py`: reuse `_on_parse_measurement_clicked`,
  `normalized_measurement_set_payload`, existing prefill and preview paths.
  Current normalized Q mapping falls back from extremes to inner bounds, losing
  the distinction. Parmasto inputs are scalar means/CVs, not mean intervals.
- `database/reference_sync_state.py::_LIBRARY_PAYLOAD_COLUMNS`,
  `_JSON_PAYLOAD_COLUMNS`, `canonical_library_payload`; and
  `database/reference_sync_reconciliation.py::_PAYLOAD_COLUMNS` explicitly select
  payload fields. New fields must be included end to end.
- Reuse `build_observation_reference_snapshot`, snapshot serialization/comparison,
  existing bundle import/export, revision/successor, and curated-fork paths.
- `docs/supabase-sync-contract.md` requires CAS revisions, authoritative baselines,
  retry-safe mutations and frozen snapshots. Follow `.claude/rules/cloud-sync.md`.
- `2026-07-12-parmasto-matching-foundation.md` prohibits manufacturing Parmasto
  statistics or midpoint means from conventional literature ranges.

## Recommended schema

Add three nullable columns to normalized `reference_measurement_sets`:

| Column | SQLite | Cloud equivalent | Purpose |
| --- | --- | --- | --- |
| `measurement_details_json` | TEXT | JSONB | Versioned, validated additional statistics and accepted range interpretation |
| `q_core_min` | REAL | Match existing Q numeric type | Independent inner Q lower bound |
| `q_core_max` | REAL | Match existing Q numeric type | Independent inner Q upper bound |

Cloud numeric columns use double precision in the reviewed migration. Confirm
current migration ownership and deployed compatibility in Stage 1; the table above is
proposed, not executable migration SQL. No new child table or new ownership/RLS
boundary is proposed. Extend existing row validation and mutation contracts.

Retain scalar `length_mean`, `width_mean`, `q_mean` as the only canonical locations
for single means. Retain current measurement-bound columns. Do not rename legacy
p05/p95 columns as part of this work. Their labels are not statistical evidence.

Use one typed, versioned JSON contract rather than arbitrary metadata. This avoids
many sparse columns for median/SD and interpretation while keeping one measurement
set as the existing atomic persistence and sync unit. A separate summary table
would add identity, revision, and sync machinery without a present need. Dedicated
mean-bound columns remain an alternative if SQL interval filtering becomes an
immediate requirement; it is not part of this request.

The JSON name is deliberately broader than “reported statistics”: it owns extra
numeric statistics and describes current interpretation of numeric columns.
Dedicated mean-bound columns would not remove cross-field semantic invariants.
Complete ordinary-range objects in JSON would only be safer if all existing
numeric columns became derived projections; that larger migration is out of scope.

Illustrative version-1 details object (length only; width and Q use the same shape):

```json
{
  "schema_version": 1,
  "metrics": {
    "length": {
      "outer_range": {"kind": "reported_extremes"},
      "core_range": {
        "kind": "percentile_interval",
        "percentile_bounds": [5, 95]
      },
      "mean_interval": {
        "lower": 8.9,
        "upper": 13.7,
        "kind": "reported_range"
      },
      "median": {
        "lower": 9.0,
        "upper": 13.9,
        "kind": "reported_range"
      },
      "sd": {"value": 0.696}
    }
  }
}
```

### Numeric ownership and minimum semantics

- `core_range` and `outer_range` describe existing numeric column pairs, never
  duplicate their values. Scalar means remain solely in the three mean columns.
- `mean_interval` owns mean interval endpoints. It is mutually exclusive with
  that metric's scalar mean. No scalar mean descriptor is required in v1.
- `median` uses either `value` or `lower`/`upper`, exclusively. SD uses `value`.
  Statistic identity follows the key, not an additional statistic-type enum.
- Core interpretation kinds: `unspecified`, `typical_range`, `reported_range`,
  `percentile_interval`. Percentile bounds are present only for percentile kind.
  Mean/median interval kinds initially support `reported_range` and `typical_range`.
  A reported range makes no claim about population, confidence, or coverage.
- Outer interpretation supports `reported_extremes`; an absent descriptor means
  the historical min/max pair's role is unknown. Presence of the JSON object alone
  must never upgrade old Q bounds to extremes.
- Meaning is per metric/statistic. A common header may initialize multiple tags.
- Remove `basis` from v1: the object records current accepted interpretation, not
  provenance history or a claim that every number is verbatim from the source.
  Keep raw text as evidence. An endpoint edit may retain the visible interpretation;
  the user can correct it. Do not silently claim source fidelity after edits.
- Defer population enums, source-label fields, per-statistic notes, provenance
  history and confidence/tolerance machinery. Missing population always means
  unknown, never individual spores or specimen means. Retain unusual source
  information in raw text; no automatic statistical use depends on these omissions.
- Ordinary legacy notes are not automatically public. Do not add arbitrary private
  notes to the new public-safe snapshot representation.

### Validation and explicit edits

One shared Python measurement-content contract owns the rules and codec; finalize
its module/API in Stage 1 after checking existing model helpers. This is one owner
of rules, not a requirement to route all transactions through repository methods.

- Validate the complete candidate: supported schema/version/enums, finite positive
  dimensions/Q, nonnegative SD, paired ordered endpoints, and scalar/interval
  exclusivity. Reject booleans as numeric values. Preserve equal-endpoint intervals.
- A percentile descriptor requires a complete core pair and bounds satisfying
  `0 <= lower < upper <= 100`. Validate outer/core ordering when outer role is
  explicitly known. Do not require mean intervals to lie inside core percentiles.
- Missing statistics are absent, not zero. SQL NULL is the canonical absence of
  the extension; normalize semantically empty supported objects to NULL.
- Clearing bounds removes their descriptor. Clearing a scalar/interval removes
  that representation; switching representation explicitly removes the other.
  L/W swap moves all numeric values and interpretation together.
- Separate edit operations from validation of authoritative imported/remote state.
  Transport must not manufacture user edits, swap history, or provenance changes.
- Unknown future versions are preserved without interpretation and are read-only;
  unsupported transfer paths reject them explicitly rather than dropping keys.

### Enforcement by path

| Path | Required enforcement |
| --- | --- |
| Repository create/update | Validate complete candidate; update uses shared transition rules |
| Successor creation | Same transition rules before copying into a new UUID/revision |
| Parser and both editors | Typed intermediate content; shared clear/switch/swap operations |
| Bundle and portable import | Validate complete incoming content; inspect omitted fields before merging |
| Curated fork | Validate/copy full supported content or reject unsupported enhanced bundle |
| Cloud pull/direct SQL upsert | Shared content validation without interactive normalization |
| Reconciliation | Atomic scientific-content conflict group, then validate resulting row |
| Snapshot build/refresh | Serialize validated content; never reinterpret or rewrite old evidence |
| Cloud mutation | Equivalent authoritative structural validation and request-presence guard |
| Unaware local binary | Enforceable database write barrier, not just new application checks |

The scientific-content conflict group initially includes measurement bounds and
means, the details object, raw text/points, sample/specimen counts, data kind,
character and measurement-method fields. Finalize the explicit field list in Stage
1. Different concurrent edits inside that group conflict unless the complete
resulting content is identical; do not auto-merge a bound edit with a tag edit.
Unrelated bibliographic/administrative fields retain current merge rules. No deep
JSON merge or per-statistic conflict engine is required in the first release.

## Migration, compatibility and round-trip safety

### Local schema and Q meaning

Add the three nullable columns idempotently in the actual normalized schema owner.
Do not migrate historical numeric values, infer tags from p05/p95 names, or mass
reparse source text. Leave existing q_min/q_max as reported bounds of unknown role.
New explicitly parenthesized extremes receive the outer tag; inner Q bounds use
q_core_min/max. Adding statistics to an old row does not relabel its outer pair.

Readers display both pairs and roles when known. Generic envelope fallback may
select one complete available pair, retaining its role; never mix endpoints from
different pairs or copy fallback numbers into persisted extreme columns.
`range_payload_is_plottable` is based on L/W, not Q: preserve it unless a concrete
consumer change requires otherwise. Audit Q display/plot translation directly.

### Small cloud compatibility mechanism

The reviewed mutation RPC is patch-style (`jsonb_populate_record`), behind a
rate-limit wrapper; it already sees omitted keys separately from explicit JSON
null and returns the authoritative row. Keep existing ownership/CAS/revision rules.

- New writers always send all three extension keys, including NULL values.
- Enhanced means any non-NULL extension column. Require all extension keys for
  content mutations of an enhanced row and creation of a successor of one.
  Apply the guard in the authoritative underlying mutation implementation.
- Inspect key presence before record population. Reject unaware mutations clearly;
  unchanged requests may retain no-op behavior. Finalize delete/restore treatment
  separately so lifecycle operations cannot become a content-write bypass.
- Explicit NULL from a capable request means clear. Omission means no extension
  contract acknowledgement. These are request properties, not durable row states.
- No global capability registry or generic client-version negotiation is proposed.
  Key presence acknowledges this contract; it is not an authorization mechanism.
- New servers must handle JSON null versus SQL NULL on insert/update themselves.
  Do not remove a NULL extension key in the adapter as the existing raw-points
  create workaround does; that would destroy the acknowledgement.
- Add the extension to all payload/read allowlists and JSON decoding registries.
  Unsupported old servers must reject enhanced writes; no lossy fallback payload.

### Local/offline compatibility — write barrier (resolved)

Before this work, normalized SQLite initialization had no forward-version write
guard: an old binary could change known columns while preserving unknown
details, and a schema-version field alone cannot protect against code that
never checks it. Stage 3A implemented the barrier described here in the
production schema owner, and the human-gated shipped v0.9.22 verification
passed (see the Stage 3A handoff).

Stage 1 selected and proved the barrier (contract §6, spike
`tests/test_measurement_content_write_barrier_spike.py`): SQLite triggers on
`reference_measurement_sets` whose body calls a connection-registered function,
`sporely_measurement_contract()`. Unaware connections fail at statement prepare
(`no such function`), which is not bypassed by clearing extension fields, by an
attached-database import, by a legacy bundle merge, by a pull-style upsert, by an
unaware successor insert, or by an old-binary table rebuild. Amended by Stage 1:
because function resolution precedes the trigger `WHEN` clause, the barrier is
coarse — an unaware binary cannot INSERT or UPDATE any row of that table once
the barrier exists (reads, deletes and all other tables are unaffected). This is
the unsupported-open policy, accepted by human review on 2026-09-11 with no
finer row-dependent compatibility to be attempted. Exercising an actual shipped
older build stays human-gated and must succeed before the persistent
local-schema/barrier sub-stage is implemented. An older binary importing an
enhanced bundle into a library never opened by an aware binary remains lossy;
that desktop is below the minimum supported version for enhanced bundles, and
supported import paths must preserve or explicitly reject. Release notes alone
are not a safety mechanism.

If safe downgrade cannot be established, define an enforceable unsupported-open
policy before release; do not merely document that users should avoid old binaries.
No promise of old-client read compatibility follows from safe rejection of writes.

### Imports, baselines and retry behavior

Existing bundle import intersects source and destination columns; portable equality
also compares intersecting keys. Neither is sufficient for enhanced content.
Unknown incoming content must survive completely or be rejected. Older input must
not revision-upgrade numeric fields while retaining newer descriptors accidentally.
Check the full scientific-content group and require an explicit supported decision
when an older source lacks enhanced fields. Do not silently call it equivalent.

Recognized historical baseline omissions may compare as NULL to avoid no-op churn.
Do not normalize missing incoming request keys to NULL before compatibility checks.
Current live retries reload canonical row state; there is not a universally frozen
pending request envelope. Preserve current UUID/CAS and unknown-create recovery
semantics; test upgrade/retry transitions rather than asserting payload immutability
that the executor does not implement.

Use one contract-owned SQLite JSON encoder, and decoded-object equality. Register
the field in sync/import JSON-field sets. Existing payload/snapshot serializers
already provide canonical handling; do not introduce a competing serialization
framework. Comparison normalization never rewrites frozen source/snapshot bytes.

### Snapshots and deployment

Define snapshot v2 for enhanced content, with the details object explicitly placed
outside the numeric-only `measurements` mapping; q_core fields belong inside that
mapping. Finalize exact shape and size limits in Stage 1. Retain v1 readers and emit
v1 for unchanged legacy-only content where practical. The details object's version
1 is independent of the enclosing snapshot version 2.

- Update Python and cloud builders/validators, curated/portable validators, and
  use-feed readers together. Current cloud validation has exact keys and numeric
  measurement values; merely appending JSON will be rejected.
- Compare v1/v2 via a version-aware semantic projection. Missing extension versus
  no extension is equal; missing extension versus real statistics is different.
  Ignore format-version differences only for supported equivalent projections.
  Unsupported future versions must not be silently projected as v1.
- Never rewrite historical snapshots or automatically enrich them on pull.
  Use existing explicit refresh/successor workflows for replacement evidence.
- Old desktop use-feed readers reject version 2, potentially blocking the whole
  feed. Ship compatible readers before enabling enhanced attachments, or establish
  an explicit safe rollout restriction. Do not silently omit enhanced uses.
- Public/share/curation builders must preserve enhanced content or reject an
  unsupported publication. Reduced projections must not masquerade as complete
  snapshots. Fully editable curated statistics may remain deferred behind a gate.

Deploy cloud acceptance/validation first, compatible readers next, then enable
writes/attachments only after all preservation and old-writer guards pass. Cloud
support may be dormant ahead of UI. Rollback disables enhanced editing while
retaining the stored extension. No destructive down migration.

## Parser and UI behavior

- Decode HTML entities before dimension splitting and normalize HTML line breaks.
  Preserve raw source. The `x` in literal `&#x20;` currently corrupts dimensions.
- Preserve table boundaries before whitespace folding. Map delimited columns by
  recognized headings, allowing missing/reordered mean, median and SD columns.
  Support existing glued Hebeloma heading via an explicit recognized pattern.
- Headerless flattened tables retain only the documented legacy layout, marked
  with unspecified meanings. Unknown/duplicate/ambiguous headers or malformed
  cells produce targeted warnings; never shift later cells into missing columns.
- Recognize Qav/Qm aliases: scalar goes into q_mean; interval into Q mean_interval.
  Do not route generic reported Q means through the Parmasto Q-mean widget.
  Named statistic extraction must not contaminate width parsing. Preserve ordinary
  Q ranges independently. Conflicting repeated named values warn rather than win
  silently. Existing mean and percentile notices become useful persisted previews.
- Extend both existing editors, not a second entry workflow. Mean editing supports
  scalar OR interval; show a compact meaning tag and accessible explanation.
  Show percentile numbers only for explicit percentile descriptors. Unknown old
  inner ranges can display “Inner range — interpretation unspecified.”
- Fix `_summary_interval` losing scalar-versus-equal-endpoint-interval shape:
  the typed parser intermediate must preserve the distinction.
- Keep median and mean visually distinguished; do not place a median into a mean
  field. Provide reported median/SD in the same statistics details area.
- First UI: compact tags, scalar-or-interval mean input, median/SD details, and
  explicit correction/clear controls. No population or provenance-history forms.
  The library manager must support the same contract or make enhanced content
  read-only. Inspect-only presentation is acceptable before richer editing.
- Include tags and intervals in summary, reopen/prefill, attachment preview and
  relevant exports. Use existing plot paths for measurement bounds; introducing
  statistical interval graphics or new matching scores is out of scope.
- No automatically derived Parmasto CV, SD, midpoint, species mean, or matching
  eligibility. Tags describe evidence; they do not by themselves qualify a record.

## Canonical stage sequence

Each stage needs a bounded prompt and independent review. Read child AGENTS.md
before cross-repository work and relevant sync/Supabase/GUI/localization rules
before touching those subsystems. The order is Stage 1 → Stage 2 → Stage 3A →
Stage 3B → Stage 3C → Stage 3D → Stage 4 → Stage 5; every stage below has one
`## Stage <label> — <title>` heading, and its section is that stage's brief.

Stage 3 as a whole is *durable storage and compatibility*: cloud schema/RPC
guards and snapshot readers may precede UI; local migration/barrier, all
authoritative writer checks, grouped reconciliation, canonical payloads,
snapshots/comparison and transfer preservation are implemented; unsupported
enhanced transfer routes reject explicitly; feature activation waits for the
complete end-to-end round-trip slice. Its reviewed execution split it into
Stage 3A (local schema and barrier), Stage 3B (local persistence, import and
reconciliation), Stage 3C (cloud schema/RPC and sync transport) and Stage 3D
(snapshot v2 and attachment/export/import transport). Only those four sections
define work; this paragraph is the umbrella, not a stage.

Self-verifiable work: contract/parser/unit tests, local fixture migrations, import
and snapshot round trips, syntax checks. Human-gated work under AGENTS.md: live
cloud/CAS/cross-client behavior and interactive edit/swap/save/restart; rendered
screenshots prove layout only. Leave human-gated candidates uncommitted until the
required manual verification is confirmed. Do not commit unrelated parser work.

## Stage 1 — Contract and compatibility fixtures

Completed: accepted at `a0bdcd3737370b307717a71e6ff2579798604ee4` (stage
`stage-reported-statistics-contract`; record in the Stage 1 handoff).

Freeze the contract and compatibility fixtures. Resolve the four blockers: exact
scientific conflict group; proven local write barrier; snapshot v2 shape and
reader rollout; import omission/revision policy. Finalize shared API, codecs,
cloud key-presence rules and lifecycle exceptions. Deliver exact writer/reader
map, representative old-client matrix, and executable acceptance cases. No
persistent schema until these decisions are independently reviewed.

Result: `docs/reference-data/measurement-content-contract.md` with its
machine-readable spec block, the nine fixtures and two test modules; the coarse
unsupported-open policy, the minimum-supported-desktop-version and
minimum-supported-reader-version gates accepted by human review on 2026-09-11.

## Stage 2 — Typed contract and parser specification

Completed: accepted at `829be09b9298ecf4e8423bb7278301af876b85d1` (stage
`stage-reported-statistics-typed-parser`; record in the Stage 2 handoff).

Implement shared validation/edit helpers and pure parser tests, including headed
partial tables, HTML entities, Qav and scalar/interval distinction. Parser
development need not wait for cloud deployment. Do not enable saving the new
output until durable storage is ready.

Result: `references/measurement_content.py` (the contract §3 surface, codec,
row helpers, validation modes, edit operations) and the extended
`references/measurement_parser.py` with `MeasurementParseResult.to_content()`.
Nothing consumes `to_content()` in production until Stage 4.

## Stage 3A — Local schema and old-client safety barrier

Completed: accepted at `04ea56d74a05b931d55106aeac70f89536d53b4b` (stage
`stage-reported-statistics-local-schema-barrier`; frozen code candidate
`68d1855f`, the accepted commit records the passed human gate; record in the
Stage 3A handoff).

Ownership: the local `reference_values.db` schema extension and the coarse
old-client write barrier, in the production schema owner
`database/reference_library_schema.py` only. Adds `measurement_details_json
TEXT`, `q_core_min REAL`, `q_core_max REAL` to `reference_measurement_sets`,
idempotently and atomically on every migration path (fresh, legacy upgrade,
CASCADE-era rebuild), with no committed state that has the columns without the
guard triggers. The barrier is exactly contract §6: BEFORE INSERT/UPDATE
triggers whose body requires the connection-registered
`sporely_measurement_contract()`; supported code identifies itself through
`register_measurement_contract` at the schema owner, `get_reference_connection`
and the portable-import `ATTACH` site. Legacy rows get NULL and no invented
semantics; DELETE and SELECT are outside the barrier.

Prerequisite, satisfied: the human-gated shipped-old-build verification
(contract §6). The shipped macOS v0.9.22 read the upgraded library, was blocked
on UPDATE and INSERT with no partial mutation, and `quick_check` stayed `ok`.

Boundaries that held: no repository, import, reconciliation, cloud, snapshot,
parser or UI change; the repository still wrote only pre-feature columns.

## Stage 3B — Local persistence, import and reconciliation wiring

Completed: accepted at `ee90fbe08953f42f1f1e1db2f4cf0b56af459d9f` (stage
`stage-reported-statistics-local-persistence`; record in the Stage 3B handoff).

Ownership: local production round-tripping of the three extension fields
through the existing owners. `MeasurementSet` / `MeasurementSetRepository`
(`_COLUMNS`, `_validate` → canonical codec + `validate_measurement_content(mode=
"edit")`, `create`, `update`, `create_revision`, `_require_editable_source` for
unsupported-version and malformed rows, inspect-only until upgrade); import
policy (contract §8) owned by `references/measurement_content.py` and applied
by bundle import (`utils/db_share.py::_upsert_library_row_by_revision`, new
no-write outcomes and `conflict` at equal revision when the §5 group differs)
and portable import (`utils/archive/portable_import.py::_merge_reference_entity`,
partial acknowledgement and predates-contract rejections, replay check); pull
reconciliation (`database/reference_sync_reconciliation.py`) fails closed with
a recorded conflict `unacknowledged_measurement_content_extension` when a
non-acknowledging remote would overwrite a locally enhanced row. Validation
precedes every statement; writes are atomic. Behaviour changes recorded in the
handoff: the validator also rejects non-finite, non-positive and inverted
ordinary numeric values, and the bundle importer reports `conflict` instead of
`skipped_same` for differing content at equal revision.

Boundaries that held and remain: `_LIBRARY_PAYLOAD_COLUMNS`, `_PAYLOAD_COLUMNS`,
`_JSON_COLUMNS`, `_MEASUREMENT_SET_KEYS` were deliberately not extended
(pushing the keys before the cloud allowlist accepts them would reject whole
feeds, §4); curated copy still produces legacy-only rows from v1 snapshots;
no parser, editor, snapshot, cloud or plotting change; `to_content()` is not
yet wired to any production save.

## Stage 3C — Cloud schema/RPC and sync transport

Implemented 2026-09-13; candidate awaiting sparring (see the Stage 3C handoff
at the top of this plan).

### Goal

Carry the three extension fields between the desktop reference library and
the Supabase reference tables with the compatibility mechanism of *Small cloud
compatibility mechanism* above, so that an enhanced row survives push, pull
and reconciliation unchanged and an unaware writer is rejected rather than
allowed to strip it. The Stage 1 contract (§3–§5, §7, §11, §12) and the Stage
2/3A/3B code are authoritative; do not reopen schema, naming, the coarse local
barrier, ontology or snapshot decisions.

### Scope — `sporely-web` (deploy first)

- Reviewed migration adding `measurement_details_json JSONB`, `q_core_min
  double precision`, `q_core_max double precision` to the normalized cloud
  `reference_measurement_sets`, nullable, no default, no backfill, no
  destructive down migration. Read the current migration ownership named in
  *Code evidence* (`20260828143513_add_normalized_reference_library.sql` and
  the public-RPC wrapper) before writing SQL.
- Row-level cross-field validation of the complete candidate row in the
  authoritative mutation path, equivalent to `validate_measurement_content`
  in `authoritative` mode (finite positive values, pair ordering, the
  4096-byte limit, supported `schema_version` accepted, unknown future version
  accepted opaquely). Not JSON-only validation.
- Key-presence guard in the authoritative underlying implementation of
  `sync_reference_measurement_set`, before `jsonb_populate_record`: a content
  mutation of an enhanced row, and creation of a successor of an enhanced row,
  require all three extension keys present; omission is rejected with the
  existing `invalid_payload` status; explicit JSON `null` clears; unchanged
  requests may keep no-op behaviour. Delete/restore and other lifecycle
  operations are exempt exactly as contract §7 step 5 / §12 cases 19–20
  state, and must not become a content-write bypass. Existing ownership, CAS
  and revision rules are unchanged.
- Curated tables: no relaxation of curated snapshot CHECKs and no change to
  `private.reference_snapshot_valid` / `private.reference_canonical_snapshot`
  in this stage (Stage 3D). Prove that the existing v1 snapshot builders are
  unaffected by rows that carry the new columns.

### Scope — `sporely-py`

- Register the fields end to end in the payload and key registries the plan
  names: `database/reference_sync_state.py::_LIBRARY_PAYLOAD_COLUMNS`,
  `_JSON_PAYLOAD_COLUMNS`, `canonical_library_payload`;
  `database/reference_sync_reconciliation.py::_PAYLOAD_COLUMNS`,
  `_JSON_COLUMNS`, `_MEASUREMENT_SET_KEYS`; `utils/reference_cloud_adapter.py`
  read allowlists and JSON decoding registries. Encoding and equality use the
  Stage 2 codec and decoded-object equality; no second serialization.
- New writers always send all three keys, including NULL. Do not strip a NULL
  extension key in the adapter (the raw-points create workaround must not be
  copied); that would destroy the acknowledgement.
- Pull reconciliation: when the remote row carries the group, apply the §5
  scientific-content conflict-group rule in `_reconcile_live` — the 26 fields
  move as one unit, concurrent edits inside the group conflict unless the
  complete resulting content is identical, unrelated fields keep current merge
  rules, and the merged row is validated before `_write_domain`. The Stage 3B
  fail-closed guard `_extension_write_blocked` stays in force for remotes that
  do not acknowledge the extension (an old server, or a pre-Stage-3C client's
  row); it is replaced only for acknowledging remotes, never removed.
- Retries and baselines (`utils/reference_cloud_sync.py::_execute_live`): the
  current retry reloads canonical row state; test upgrade/retry transitions
  across an acknowledging request rather than asserting payload immutability.
  Recognized historical baseline omissions compare as NULL; incoming request
  keys are never normalized to NULL before compatibility checks.
- Unsupported old servers must reject enhanced writes; there is no lossy
  fallback payload. Cloud support may be dormant ahead of UI: nothing in this
  stage enables enhanced attachments or editor saving.

### Verification expectations

- Contract §12 cases that concern cloud transport, executed against the
  production owners; the *Required regression matrix* items for explicit
  extension NULL vs omitted keys, create/update/clear, enhanced-predecessor
  successor requests, no-op and delete/restore, baselines and retries across
  upgrade, unknown-create recovery, stable JSON object equality,
  unchanged-record no-op, CAS and pull-only zero writes, and grouped conflict
  vs unrelated merge.
- Round trip: an enhanced local row pushed, pulled into a second library and
  reconciled is byte-identical in canonical form; a legacy row acquires no
  semantics; a future-version details object survives opaquely.
- `sporely-web`: the repository's own migration and RPC test conventions,
  including a rejected unaware mutation with `invalid_payload`, an accepted
  lifecycle operation on an enhanced row, and validation rejections.
- Desktop: focused sync/reconciliation/adapter modules, Stage 3A/3B schema,
  barrier and persistence regressions, `py_compile`, `git diff --check`, full
  suite against the PROJECT.md baseline with exact counts.
- Human-gated under AGENTS.md: live cloud/CAS/cross-client behaviour, and
  that a deployed pre-Stage-3C server rejects an enhanced write. Do not claim
  either without the branch owner's evidence.

### Hard boundaries

Do not: implement snapshot v2 or change any snapshot version, builder or
comparison; enable enhanced attachment transport or the use feed for v2;
change the Stage 3A local schema or weaken the barrier; change the coarse
old-client policy; change parser, editor, plotting or matching behaviour;
derive Parmasto statistics, CVs, midpoint means or eligibility; relax curated
CHECKs; add a capability registry or client-version negotiation; write a
destructive down migration; commit unrelated work.

### Handoff

Report exact files changed in both repositories, the migration and RPC
ownership, the exact guard and validation rules as implemented, every registry
extended, how `_reconcile_live` applies the group rule and where the fail-closed
guard still applies, focused/full-suite verification with baseline failures
separated, human-gated items still pending, and the candidate SHA per
repository.

## Stage 3D — Snapshot v2 and attachment/export/import transport

Implemented 2026-09-14; sparring returned `READY` for both candidates and the
human gate passed, so acceptance and merge are the branch owner's remaining
decisions (see the Stage 3D handoff at the top of this plan for the full
record). Stage 3C was accepted at `3c0f65b5` / `b32eb922`, the base in each
repository. Owns the frozen-evidence representation of enhanced content and
the gates that protect old readers.

Prerequisite, satisfied: the human-gated pre-activation reader check
(`pre-activation-desktop-v2-feed`, contract section 7 rollout step 1). The
oldest desktop build that will remain supported pulled a complete
observation-use feed containing a valid version-2 snapshot from a synthetic
account, and neither rejected the feed nor omitted the row. Version-2
emission and enhanced attachments nevertheless stay disabled: passing the
check is what makes opening the gate *permissible*, not what opens it.

Implementation record:

- Desktop: `database/reference_citation.py` emits v1/v2 by the row's enhanced
  state and owns `snapshot_semantic_projection`;
  `database/reference_library.py::_gated_observation_reference_snapshot`
  applies the reader gate at the attachment boundary (`_do_attach`,
  `snapshot_status`, `refresh_snapshot`, and `successor_status`, which reports
  an enhanced successor as `unsupported`) and maps a malformed stored details
  object to `ReferenceIntegrityError`;
  `stage_observation_reference_use_feed`, `curated_reference_forks` and
  `utils/archive/portable_import.py` accept v1/v2 with version-keyed exact key
  sets; `copy_curated_bundle_to_personal_library` copies all three extension
  fields or rejects; `references/measurement_content_gates.py` holds both
  gates, closed, and `utils/db_share.py` /
  `utils/archive/portable_export.py` refuse to produce an enhanced archive
  while the desktop-version gate is closed.
- Cloud: migration
  `20260914090000_extend_reference_snapshots_to_version_2.sql` in
  `sporely-web` (candidate `1bb5c804`) makes `private.reference_snapshot_valid`
  version-keyed, `private.reference_canonical_snapshot` emit v2 for an
  enhanced row, `private.public_reference_snapshot` preserve the extension,
  relaxes the curated publication CHECK and the public curated reader to
  `IN (1, 2)`, and gives the curation-intake candidate the three keys under
  `measurement_set` (all-or-none). Applied and tested on the local stack;
  deployment to the project remains the branch owner's step.
- Deferred and named: curated *storage* of the extension
  (`private.curated_reference_measurement_sets` has no extension columns, so
  the curation pipeline still publishes v1 bundles; the desktop copy path is
  reached with v2 through shared contributions, which carry the canonical
  snapshot directly).

- Snapshot v2 with the shape frozen by Stage 1 (contract §9 / blocker 3):
  details object outside the numeric-only `measurements` mapping, `q_core_*`
  inside it, 65536/4096-byte limits; `details_to_object` is the embedded form.
  Update the Python builder (`build_observation_reference_snapshot`), cloud
  validators and canonical/public/share/curation builders
  (`private.reference_snapshot_valid`, `private.reference_canonical_snapshot`,
  curated CHECK relaxation) and curated/portable validators together.
- Version-aware semantic projection for v1/v2 comparison
  (`database/reference_citation.py::observation_snapshots_semantically_equal`):
  missing extension equals no extension; missing versus real statistics
  differs; unsupported future versions are never projected as v1. Emit v1 for
  unchanged legacy-only content where practical. Never rewrite or enrich
  historical snapshots; replacement evidence goes through the existing
  refresh/successor workflows.
- Readers first: the desktop use-feed reader
  (`database/reference_use_sync_reconciliation.py::stage_observation_reference_use_feed`)
  accepts v2 before any v2 is emitted; curated copy
  (`copy_curated_bundle_to_personal_library`) preserves v2 content or rejects
  the unsupported bundle; bundle/portable/public transfer paths preserve or
  reject, never intersect away.
- Gates as mechanisms: the minimum-supported-reader-version gate for v2
  emission and enhanced attachments, and the minimum-supported-desktop-version
  policy for enhanced bundles, implemented so that activation is an explicit,
  reversible switch; this stage ships them disabled. Rollback disables enhanced
  editing while retaining the stored extension.
- Verification: the *Required regression matrix* v1/v2 items, transfer round
  trips, preserve-or-reject behaviour, curated/public builders; human-gated:
  an actual older desktop reading a feed that contains v2.
- Boundaries: no editor/UI change, no plotting or matching, no activation of
  enhanced attachments, no change to Stage 3A–3C decisions.

## Stage 4 — Editor and UI inspection and guarded editing

Future stage; starts after Stage 3D is accepted. Minimal inspection and guarded
editing: add tags and reported values to existing preview/reopen/attachment
paths. Wire compact entry editing and either equivalent library-manager editing
or read-only enhanced content. Separate generic Q means from Parmasto input. No
new scoring/interval plots. The minimum-supported-reader-version gate and the
old-writer rollout gates must pass before enhanced attachments are enabled.

- Owns the parser → repository wiring deferred by Stage 3B: both editors
  (`ui/reference_entry_editor.py::_build_measurement_set`,
  `ui/reference_library_manager_dialog.py::_on_save`) build a `MeasurementSet`
  from `to_content()` through the Stage 2 edit operations; no second entry
  workflow.
- Follows *Parser and UI behavior* above in full: scalar-or-interval mean
  input, compact meaning tags with accessible explanation, percentile numbers
  only for explicit descriptors, median and SD in the details area kept apart
  from means, explicit correction/clear controls, `_summary_interval` shape
  fix, inspect-only presentation acceptable in the library manager before
  richer editing, tags and intervals in summary/reopen/prefill/attachment
  preview and relevant exports.
- Human-gated under AGENTS.md: interactive edit/swap/save/restart; rendered
  screenshots prove layout only. Leave human-gated candidates uncommitted until
  the manual verification is confirmed.
- Boundaries: no population or provenance-history forms, no automatically
  derived Parmasto CV, SD, midpoint, species mean or matching eligibility, no
  new plots; tags describe evidence and do not qualify a record.

## Stage 5 — Independent final review and activation decision

A fresh top-level reviewer verifies frozen candidate SHAs and repository state
across `sporely-py` and `sporely-web`, the complete end-to-end round-trip slice
(local → cloud → second client → snapshot → reader) and every gate in this plan.
No automatic merge. The activation of v2 emission and enhanced attachments is
this stage's explicit decision, taken only when the minimum-supported-reader
and old-writer gates have passed. The handoff sections of this plan are updated
every pass with exact scope, verification, deferred work and SHA.

## Required regression matrix

- Exact Hebeloma table, glued/separated headers, browser tabs, Markdown, missing
  mean, range-only and reordered columns, malformed/duplicate headings/cells.
- Both supplied strings, literal/decoded entities, scalar Qav, interval Qav,
  simultaneous Q/Qav and duplicate conflicts; no width contamination.
- Mean/median distinction, equal-endpoint interval preserved, clear/switch/swap,
  independent L/W/Q tags, invalid numeric values/version/enums, no inferred means.
- Legacy migration twice; unchanged historical Q values/unknown role; no inferred
  percentile tags. Q pair fallback never mixes roles or writes derived extremes.
- Every mutation path in the enforcement table, including library-manager edits,
  successor copy, direct pull, forks, and higher-revision older imports.
- Concurrent bound/tag edits conflict as one scientific group; unrelated changes
  still merge. Identical scientific content does not create spurious conflicts.
- Explicit extension NULL vs omitted request keys; create/update/clear and
  enhanced predecessor successor requests; no-op and delete/restore cases.
- Actual supported older clients: cloud write rejection, local write barrier,
  startup/downgrade behavior, imports and attached databases, future-version safety.
- v1/v2 snapshots: missing-vs-empty equivalence, real-statistics difference, frozen
  evidence unchanged, explicit refresh, unsupported-version feed behavior.
- Cloud/local/bundle/portable round trips and curated/public preserve-or-reject
  behavior; no unknown-column intersection silently drops content.
- Baselines and retries across upgrade, unknown-create recovery, stable JSON object
  equality, unchanged-record no-op behavior, CAS and pull-only zero writes.
- Negative plotting/Parmasto checks: intervals, median/SD and tags never populate
  scalar mean or Parmasto inputs. Existing scalar consumers remain explicit.

## Deferred without compromising the first slice

Population ontology, provenance/edit history, formal confidence/tolerance intervals,
SQL mean-interval filtering, new plots and matching, editable advanced details,
and full curated authoring support. Existing transfer/publication paths must first
preserve enhanced content or reject it explicitly; their safety cannot be deferred.

## Code evidence and review decisions

These are inspected source locations, not proof of deployed behavior. Line numbers
may move during implementation; symbols identify the ownership boundary.

| Finding | Evidence |
| --- | --- |
| Repository merges partial updates and successors | `database/reference_library.py::MeasurementSetRepository.update/create_revision` |
| Pull bypasses repository and merges individual fields | `database/reference_sync_reconciliation.py::_write_domain/_reconcile_live` |
| Bundle importer intersects columns | `utils/db_share.py::_upsert_library_row_by_revision` |
| Portable comparison intersects keys | `utils/archive/portable_import.py::_equivalent_rows` and revision-upgrade path |
| Curated fork has explicit SQL fields and strict snapshot shape | `database/curated_reference_forks.py::copy_curated_bundle_to_personal_library` and snapshot validator |
| Entry swap only swaps visible cells; Q means enter Parmasto widget | `ui/reference_entry_editor.py::_on_swap_lw_clicked/_set_parsed_result` |
| Second editor parses and saves normalized values | `ui/reference_library_manager_dialog.py`, parser mapping near 1770 and repository writes near 1905 |
| Named scalar consumers isolate plotting/Parmasto | `references/reference_plotting.py::_translate_range_or_summary`; `ui/main_window.py::_parmasto_reference_metrics` |
| Fixed projections and JSON registries | `database/reference_sync_state.py::canonical_library_payload`; `utils/reference_cloud_adapter.py` |
| Live retry reloads row payload | `utils/reference_cloud_sync.py::_execute_live` |
| Snapshot comparison ignores revision only | `database/reference_citation.py::observation_snapshots_semantically_equal` |
| Desktop use pull rejects versions other than 1 | `database/reference_use_sync_reconciliation.py::stage_observation_reference_use_feed` |
| No normalized forward-version guard | `database/reference_library_schema.py::init_reference_library_schema` |
| Cloud patch RPC, exact snapshot shape and canonical builder | `sporely-web/supabase/migrations/20260828143513_add_normalized_reference_library.sql`, `sync_reference_measurement_set`, `private.reference_snapshot_valid`, `private.reference_canonical_snapshot` |
| Current public RPC wraps renamed implementation | `sporely-web/supabase/migrations/20260830193144_configure_shared_reference_production_policy.sql` |

### Disposition of the 15 adversarial challenges

A = resolve before schema implementation; B = decide now; C = safely deferred.

| Challenge | Classification and resolution |
| --- | --- |
| 1. Persistence boundary | A/B: retain JSON plus Q core; no numeric duplication; enforce coupled content |
| 2. Invariant ownership | A: shared rule owner across multiple transactional writers, grouped merge |
| 3. Name | B: measurement_details_json covers interpretation and additional values |
| 4. Semantic richness | B/C: keep essential kinds/shape, defer population and provenance fields |
| 5. Source versus interpretation | A: remove basis; current interpretation is not source certification |
| 6. Old clients | A: RPC key-presence guard; prove enforceable local protection |
| 7. Omission/NULL | A: request-boundary distinction, complete stored state, explicit import policy |
| 8. Q roles | A: unknown historical outer role, explicit extremes tag, role-preserving pair fallback |
| 9. Equal endpoints | B: preserve interval shape; fix parser collapse |
| 10. Consumer isolation | A/C: fix Q/Parmasto adapter boundary; defer new scoring |
| 11. Parser sequence | B: contract first, parser tests before persistence deployment is acceptable |
| 12. UI scope | B/C: compact correction controls, details/read-only options, both editors covered |
| 13. Canonicalization | B: shared codec and existing JSON-aware comparisons, no new framework |
| 14. Snapshots | A: version-aware v1/v2 projection and explicit reader rollout |
| 15. Scope | A/C: durable preserve-or-reject slice first; advanced features deferred |

Next reviewer should decide whether Stage 1 concretely resolves the four blockers,
not reopen accepted naming or ontology choices without new evidence. The local
write-barrier candidate and snapshot rollout remain unproven until that work runs.
