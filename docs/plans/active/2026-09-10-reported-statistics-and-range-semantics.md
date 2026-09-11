# Reported statistics and explicit range semantics

## Current stage / reviewer handoff — 2026-09-11

Status: revised design following adversarial review; no implementation in this pass.
The user requested incorporation of the review findings. This document supersedes
its 2026-09-10 design. It is the canonical plan for subsequent statistics work;
`2026-09-09-reference-measurement-table-parser.md` retains the existing parser-stage
verification record.

Review baseline: `feature/reference-save-and-plot`, HEAD
`8097bc8f9689a730b1cfb9991ff3874eae3d2930`. This is review context, not a candidate
implementation SHA. Existing uncommitted parser/test work is preserved.
Candidate SHA: none. Verification this pass: document consistency and
`git diff --check`; no application tests or schema/cloud operations.

The prior read-only review inspected desktop code and cloud migration/RPC
contracts in canonical `sporely-web`, not deployed database state. Probes confirmed
that adding a NULL snapshot field changes current snapshot equality, unknown keys
are dropped by canonical payload projection, and missing known keys raise KeyError.
These findings are inputs to the design, not evidence that the proposed fixes work.

Decisions retained: one JSON extension plus Q core columns; no duplicate ordinary
range numbers; no inferred scalar means or Parmasto eligibility. Changes: smaller
semantics, explicit invariant ownership and conflict grouping, request-key cloud
compatibility, snapshot v2 rollout, and lossless transfer gates.

Four blockers must be resolved before schema implementation: cross-field merge
rules, enforceable local older-client write protection, snapshot read/version
compatibility, and omission handling in imports. Stage 1 resolves these concretely.

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

Stage 1 must select and verify an enforceable barrier. A candidate is a narrowly
scoped SQLite trigger requiring a connection-registered contract capability for
enhanced-row content mutations. Unaware connections fail closed. This is proposed,
not existing infrastructure or a proven solution. Audit ordinary connections,
attached-database import connections, direct SQL writers, successor operations,
and old startup migrations; test actual supported older binaries. The barrier
must not be bypassed by clearing extension fields or by a legacy import merge.

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
4. **Minimal inspection and guarded editing.** Add tags and reported values to
   existing preview/reopen/attachment paths. Wire compact entry editing and either
   equivalent library-manager editing or read-only enhanced content. Separate
   generic Q means from Parmasto input. No new scoring/interval plots. Snapshot
   reader and old-writer rollout gates must pass before enhanced attachments.
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
