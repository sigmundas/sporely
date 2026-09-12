# Reported statistics and explicit range semantics

## Current stage / reviewer handoff — 2026-09-12 (Stage 2)

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
  candidate `7e64508a` received a sparring SEND_BACK; the corrections below
  are a new commit on top of it (no history rewritten).

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
4. *Numeric prefixes leaked into width.* A named value must be a complete
   numeric token followed by a boundary (`(?![A-Za-z0-9.%])`); `Qav = 1.5e2`,
   `1.5%`, `1.5x`, `12abc` are rejected whole with a targeted warning and
   removed from the remainder, leaving width intact.

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
  with Qm warns and keeps both; named values are matched as numeric tokens so
  they cannot swallow a following dimension. New result fields
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
  and Stage 2 cases appended to `tests/test_measurement_parser.py` (86
  collected cases, 59 new; the existing 27 unchanged). The Stage 1 fixture
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
modules `test_measurement_content` + `test_measurement_parser` 167 passed;
with the Stage 1 fixture and barrier-spike modules, both editor test modules
and `test_reference_library_{schema,repository}` 319 passed;
reference-library regressions
(`test_reference_library_{schema,repository,snapshot,pull_reconciliation,bundle_roundtrip,manager_dialog}`,
`test_curated_reference_forks`, `test_legacy_reference_migration`,
`test_reference_add_dialog_normalized`) 181 passed; `py_compile` on the four
touched files ok; `git diff --check` clean. Full suite
(`--continue-on-collection-errors`, as for Stage 1): **31 failed,
4125 passed, 10 skipped, 55 errors, exit 1** (first candidate:
31 / 4100 / 10 / 55) against the Stage 1 report of 31 failed / 3983 passed /
10 skipped / 55 errors. The failing and erroring
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

### Local/offline compatibility — unresolved pre-schema blocker

Current normalized SQLite initialization has no forward-version write guard.
An old binary can change known columns while preserving unknown details. A new
schema-version field alone cannot protect against code that never checks it.

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

## Revised implementation sequence and acceptance

Each stage needs a bounded prompt and independent review; no application work is
part of this plan revision. Read child AGENTS.md before cross-repository work and
relevant sync/Supabase/GUI/localization rules before touching those subsystems.

1. **Freeze the contract and compatibility fixtures.** Resolve the four blockers:
   exact scientific conflict group; proven local write barrier; snapshot v2 shape
   and reader rollout; import omission/revision policy. Finalize shared API, codecs,
   cloud key-presence rules and lifecycle exceptions. Deliver exact writer/reader
   map, representative old-client matrix, and executable acceptance cases. No
   persistent schema until these decisions are independently reviewed.
2. **Typed contract and parser specification.** Implement shared validation/edit
   helpers and pure parser tests, including headed partial tables, HTML entities,
   Qav and scalar/interval distinction. Parser development need not wait for cloud
   deployment. Do not enable saving the new output until durable storage is ready.
3. **Durable storage and compatibility, in bounded sub-stages.** Cloud schema/RPC
   guards and snapshot readers can precede UI. Implement local migration/barrier,
   all authoritative writer checks, grouped reconciliation, canonical payloads,
   snapshots/comparison and transfer preservation. Unsupported enhanced transfer
   routes must explicitly reject. Review exact cloud/local sub-stage boundaries;
   feature activation waits for the complete end-to-end round-trip slice.
   **Prerequisite:** the persistent local-schema/barrier sub-stage does not
   start until the human-gated shipped-old-build verification (contract §6)
   has succeeded and is recorded in this handoff.
4. **Minimal inspection and guarded editing.** Add tags and reported values to
   existing preview/reopen/attachment paths. Wire compact entry editing and either
   equivalent library-manager editing or read-only enhanced content. Separate
   generic Q means from Parmasto input. No new scoring/interval plots. The
   minimum-supported-reader-version gate and the old-writer rollout gates must
   pass before enhanced attachments are enabled.
5. **Independent final review and activation decision.** A fresh top-level reviewer
   verifies frozen candidate SHAs and repository state. No automatic merge. Update
   this handoff every pass with exact scope, verification, deferred work and SHA.

Self-verifiable work: contract/parser/unit tests, local fixture migrations, import
and snapshot round trips, syntax checks. Human-gated work under AGENTS.md: live
cloud/CAS/cross-client behavior and interactive edit/swap/save/restart; rendered
screenshots prove layout only. Leave human-gated candidates uncommitted until the
required manual verification is confirmed. Do not commit unrelated parser work.

### Required regression matrix

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

### Deferred without compromising the first slice

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
