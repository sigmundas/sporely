# Measurement content contract (frozen, Stage 1)

Status: frozen decisions for the reported-statistics work. Requirements and
motivation live in
`docs/plans/active/2026-09-10-reported-statistics-and-range-semantics.md`;
this document states what Stage 2 and Stage 3 build. Every claim about current
code cites the symbol that shows it. Fixtures live in
`tests/fixtures/reference_statistics/`; `tests/test_measurement_content_contract_fixtures.py`
pins them to the rules below and `tests/test_measurement_content_write_barrier_spike.py`
proves the write barrier in section 6.

Nothing in this document changes persisted schema. The extension columns
(`measurement_details_json TEXT`, `q_core_min REAL`, `q_core_max REAL` on
`reference_measurement_sets`) are Stage 3 work in
`database/reference_library_schema.py::init_reference_library_schema`.

Terminology: a row is **enhanced** when any extension column is non-NULL.
A request or import row **acknowledges** the contract when all three extension
keys are present (values may be NULL). "Extension" means the three columns.

## 1. Details object, version 1

`measurement_details_json` stores one JSON object or SQL NULL. Version 1:

```json
{"schema_version": 1, "metrics": {"length": {...}, "width": {...}, "q": {...}}}
```

- Top-level keys are exactly `schema_version` (integer 1) and `metrics`.
- `metrics` maps a subset of `length`, `width`, `q` to metric objects. It must
  be non-empty and every metric object must be non-empty; a semantically empty
  object is normalized to SQL NULL by the codec and is invalid in stored form.
- Metric object keys are a subset of `outer_range`, `core_range`,
  `mean_interval`, `median`, `sd`. Unknown keys are invalid in version 1.
- Statistic identity follows the key; there is no statistic-type enum and no
  `basis`, population, provenance or note field (plan: *Disposition*, items 3–5).

| Key | Shape | Enum values |
| --- | --- | --- |
| `outer_range` | `{"kind": K}` | `reported_extremes` |
| `core_range` | `{"kind": K}` or `{"kind": "percentile_interval", "percentile_bounds": [lo, hi]}` | `unspecified`, `typical_range`, `reported_range`, `percentile_interval` |
| `mean_interval` | `{"lower": x, "upper": y, "kind": K}` | `reported_range`, `typical_range` |
| `median` | `{"value": v}` **or** `{"lower": x, "upper": y, "kind": K}` | interval kinds as above |
| `sd` | `{"value": v}` | — |

Number rules: JSON numbers only; booleans are rejected as numbers
(`isinstance(x, bool)` check, as `database/curated_reference_forks.py::_validate_snapshot`
already does for counts). Dimensions and Q values are finite and positive; SD is
finite and non-negative; `lower <= upper` (equal endpoints are preserved as an
interval, never collapsed to a scalar); `0 <= lo < hi <= 100` for percentile
bounds, present only for kind `percentile_interval`.

`unspecified` is an explicit tag meaning "examined, meaning unknown". An
**absent** descriptor means the historical pair was never tagged. Both display
alike; only the former is a statement by the user or parser.

Fixture: `details_hebeloma_v1.json` is the plan's Hebeloma table as a v1
object. `details_unsupported_future_version.json` (schema_version 2) is the
opaque-future example.

## 2. Numeric ownership and cross-field rules

Descriptors describe existing column pairs; they never carry the pair's values.

| Metric | Outer pair | Core pair | Scalar mean |
| --- | --- | --- | --- |
| length | `length_min`, `length_max` | `length_core_min`, `length_core_max` | `length_mean` |
| width | `width_min`, `width_max` | `width_core_min`, `width_core_max` | `width_mean` |
| q | `q_min`, `q_max` | `q_core_min`, `q_core_max` (new) | `q_mean` |

Rules validated on the complete candidate row (`MeasurementSet` fields plus
decoded details), in this order:

1. `outer_range` present ⇒ that metric's outer pair is complete and ordered.
2. `core_range` present ⇒ that metric's core pair is complete and ordered.
3. Both present ⇒ `outer_min <= core_min` and `core_max <= outer_max`.
   Ordering across pairs is validated only when the outer role is explicit.
4. `mean_interval` present ⇒ that metric's scalar mean column is NULL.
5. `median` uses `value` or `lower`/`upper`, exclusively.
6. Mean intervals are **not** required to lie inside the core pair.
7. Historical `q_min`/`q_max` on rows without an `outer_range` descriptor keep
   their unknown role. Adding statistics to such a row never adds the outer tag.
8. Extension NULL is the canonical absence; `q_core_min`/`q_core_max` may be
   set without a details object (an untagged inner Q pair).

Fixtures: `row_enhanced.json` (means NULL, percentile core, tagged extremes,
q core pair set), `row_legacy_only.json` (extension NULL, Q pair of unknown
role). Rejection cases are enumerated as test parameters in
`test_invalid_mutations_of_the_example_are_rejected`.

## 3. Shared contract module and API

**Location:** `references/measurement_content.py` (pure Python; the
`references` package imports nothing heavier than `json`, `math`, `re`;
`database/` modules currently import nothing from `references/`, so the
dependency direction `database → references` is new but acyclic).

**Owner of rules and codec**, not of transactions (plan: *Validation and
explicit edits*). Public surface:

```python
MEASUREMENT_DETAILS_SCHEMA_VERSION = 1
SUPPORTED_DETAILS_VERSIONS = frozenset({1})
MEASUREMENT_DETAILS_MAX_BYTES = 4096            # canonical encoding
EXTENSION_FIELDS = ("measurement_details_json", "q_core_min", "q_core_max")
SCIENTIFIC_CONTENT_FIELDS: frozenset[str]        # section 5
METRICS = ("length", "width", "q")
OUTER_RANGE_KINDS, CORE_RANGE_KINDS, INTERVAL_KINDS: frozenset[str]

class MeasurementContentError(ValueError)

@dataclass(frozen=True) class RangeDescriptor(kind: str, percentile_bounds: tuple[float, float] | None = None)
@dataclass(frozen=True) class IntervalStatistic(lower: float, upper: float, kind: str)
@dataclass(frozen=True) class ScalarStatistic(value: float)
@dataclass(frozen=True) class MetricDetails(outer_range, core_range, mean_interval, median, sd)
@dataclass(frozen=True) class MeasurementDetails(metrics: Mapping[str, MetricDetails])
@dataclass(frozen=True) class UnsupportedMeasurementDetails(schema_version: int, raw: Mapping[str, Any])
@dataclass class MeasurementContent          # typed intermediate: the 26 fields of section 5,
                                             # details as MeasurementDetails | UnsupportedMeasurementDetails | None

def decode_measurement_details(text: str | None) -> MeasurementDetails | UnsupportedMeasurementDetails | None
def encode_measurement_details(details) -> str | None          # None for None/empty; Unsupported re-encodes raw
def measurement_details_equal(a: str | None, b: str | None) -> bool
def content_from_row(row: Mapping[str, Any]) -> MeasurementContent
def content_row_updates(content: MeasurementContent) -> dict[str, Any]
def validate_measurement_content(content: MeasurementContent, *, mode: Literal["edit", "authoritative"]) -> None
                                             # sections 1–2, raises MeasurementContentError; mode rules below
def is_enhanced_row(row: Mapping[str, Any]) -> bool
def acknowledgement_state(row: Mapping[str, Any]) -> Literal["absent", "complete", "partial"]
def acknowledges_extension(row: Mapping[str, Any]) -> bool   # == (acknowledgement_state(row) == "complete")
# explicit edit operations (plan: clear/switch/swap); each returns a new MeasurementContent
def clear_pair(content, metric, which: Literal["outer", "core"])
def clear_statistic(content, metric, statistic: Literal["mean", "median", "sd"])
def set_scalar_mean(content, metric, value)          # removes mean_interval
def set_mean_interval(content, metric, lower, upper, kind)   # clears scalar mean
def swap_length_width(content)                        # moves numbers and descriptors together
```

**Validation modes.** `mode` is a required keyword. Both modes apply
sections 1–2 in full to supported details and differ only for
`UnsupportedMeasurementDetails` (decoded object whose `schema_version` is not
in `SUPPORTED_DETAILS_VERSIONS`): `mode="authoritative"` (pull
`stage_reference_library_feed`, `_reconcile_live`, bundle import
`_upsert_library_row_by_revision`, portable import `_merge_reference_entity`)
accepts it opaquely, enforces only descriptor-independent numeric rules (pair
ordering), and `encode_measurement_details` re-encodes `raw` unchanged;
`mode="edit"` (`MeasurementSetRepository._validate`, both editors, parser
output) raises `MeasurementContentError("unsupported measurement details
version")`, so the row is inspect-only until the binary is upgraded. Curated
copy is the one authoritative-state path that does **not** accept opaque
details: `copy_curated_bundle_to_personal_library` creates a fresh local graph
the user is expected to edit, so it validates the bundle's snapshot details
with `mode="edit"` semantics and raises `CuratedReferenceError` on an
unsupported version (section 8), never writing a degraded row. Opaque details
take part in snapshot projection as decoded
objects (section 7 compares `measurement_details` by object equality). The
cloud accepts only versions its validator knows (section 9 item 7), so a
future version reaches a desktop only from a newer server or bundle.

**Acknowledgement states** (`acknowledgement_state`): `absent` when none of
`EXTENSION_FIELDS` is present as a key, `complete` when all three are,
`partial` otherwise. `partial` is always an error on every path (section 8,
section 9 item 3): an exporter or client that knows some but not all of the
extension cannot be trusted to have preserved any of it.

**SQLite-side pieces live with the schema owner**, not in the pure module:
`database/reference_library_schema.py::register_measurement_contract(conn)`
and the barrier DDL (section 6). Snapshot projection lives with the snapshot
owner: `database/reference_citation.py::snapshot_semantic_projection`.

**Single JSON codec.** `encode_measurement_details` uses
`json.dumps(obj, ensure_ascii=True, sort_keys=True, separators=(",", ":"))`,
the same settings as `database/reference_sync_state.py::_canonical_json`.
Stage 3 routes the `measurement_details_json` value of
`database/reference_sync_reconciliation.py::_domain_values` (which today calls
`json.dumps(..., ensure_ascii=True, separators=(",", ":"))` without
`sort_keys`) through this encoder. Stored text of `row_enhanced.json` is the
canonical form; the fixture test asserts it.

**Equality rule.** Two details values are equal iff their decoded objects are
equal (`json.loads(a) == json.loads(b)`, NULL equals NULL). Byte comparison is
never used for semantics. Comparison never rewrites stored or snapshot bytes.
Registration needed for decoded comparison to hold on every path:
`database/reference_sync_state.py::_JSON_PAYLOAD_COLUMNS`,
`database/reference_sync_reconciliation.py::_JSON_COLUMNS`, the `json_fields`
sets passed for `reference_measurement_sets` in
`utils/archive/portable_import.py` (`{"raw_points_json"}` at the two call
sites in `_merge_reference_graph` and the replay check near line 1323).

## 4. Payload and key registries to extend (Stage 3 checklist)

All three extension keys are added, end to end, to:
`database/reference_library.py::MeasurementSet` and
`MeasurementSetRepository._COLUMNS`;
`database/reference_sync_state.py::_LIBRARY_PAYLOAD_COLUMNS["measurement_set"]`
and `_JSON_PAYLOAD_COLUMNS`;
`database/reference_sync_reconciliation.py::_PAYLOAD_COLUMNS["measurement_set"]`
and `_JSON_COLUMNS`; `utils/reference_cloud_adapter.py::_MEASUREMENT_SET_KEYS`;
`database/curated_reference_forks.py::_MEASUREMENT_KEYS`/`_SNAPSHOT_KEYS`
(version-aware, section 7); the explicit INSERT column list in
`copy_curated_bundle_to_personal_library`; the cloud allowlist in
`sync_reference_measurement_set` (`reference_payload_has_unknown_keys` call,
`sporely-web` migration `20260828143513`, line 573) and its INSERT/UPDATE
column lists. Missing any one of them either drops content
(`canonical_library_payload` projects only listed columns) or rejects the
whole feed (`_payload_from_mapping` does `row[column]` and raises on absence).

## 5. Scientific-content conflict group (blocker 1)

`SCIENTIFIC_CONTENT_FIELDS` (26 fields):

```
character, data_kind, raw_text,
length_min, length_core_min, length_core_max, length_max,
width_min, width_core_min, width_core_max, width_max,
q_min, q_core_min, q_core_max, q_max,
q_mean, length_mean, width_mean,
sample_size, specimen_count,
mount_medium, stain, preparation, measurement_method,
raw_points_json, measurement_details_json
```

Justification against current columns (`_REFERENCE_MEASUREMENT_SETS_DDL`) and
payload (`_PAYLOAD_COLUMNS["measurement_set"]`):

- Included: every numeric column plus the three extension fields, because
  descriptors describe pairs and means exclude intervals (section 2); a merge
  that takes a bound from one side and a tag from the other yields a row no
  writer produced. `raw_text` and `raw_points_json` are the evidence the
  numbers were derived from. `sample_size`/`specimen_count` and the four
  method fields qualify the numbers and appear in the snapshot
  (`build_observation_reference_snapshot`, `measurements` and `method`
  mappings). `character` and `data_kind` select the interpretation of every
  other field.
- Excluded: `id`, `taxon_treatment_id`, `supersedes_id` are identity and
  already conflict unconditionally via `_IDENTITY_FIELDS["measurement_set"]`
  in `_reconcile_live` (line 513). `notes` is private administrative text,
  excluded from the snapshot (`build_observation_reference_snapshot` omits
  it) and keeps per-field merging. `revision` is derived
  (`_reconcile_live`, lines 544–548). `legacy_reference_value_id`,
  `created_at`, `updated_at` are not payload columns.

**Rule** (replaces the per-field overlap test in `_reconcile_live`,
lines 510–524 and 541–549):

1. Compute `local_changes` and `remote_changes` against the baseline as today.
2. Expand: if a change set intersects the group, replace that intersection
   with the whole group.
3. Conflict iff an identity field changed remotely, or any field in
   `expanded_local ∩ expanded_remote` differs between local and remote.
   Consequently: both sides touched the group and the *complete* group content
   is identical ⇒ no conflict; both touched it and anything differs ⇒ conflict
   with `overlapping_fields` = differing group fields. A bound edit versus a
   tag edit therefore conflicts; identical scientific content does not.
4. Merge: for each field in `expanded_local_changes`, take local; otherwise
   remote. The group moves as a unit; `notes` and bibliographic fields keep
   per-field behaviour. Then validate the resulting row through
   `validate_measurement_content` before `_write_domain`; a validation failure
   is recorded as a conflict, never written.

`MeasurementSetRepository.update` (merges `updates` into `asdict(existing)`)
and `create_revision` (copies `asdict(existing)` then applies `updates`) are
single-writer paths and need validation of the merged candidate, not the group
rule. No deep JSON merge exists or is planned.

## 6. Local write barrier (blocker 2)

**Selected barrier: trigger plus connection-registered function.** Two
`BEFORE` triggers on `reference_measurement_sets` call the application-defined
SQL function `sporely_measurement_contract()`; a contract-aware connection
registers it with `sqlite3.Connection.create_function` returning the local
contract version (1). The triggers RAISE(ABORT) when the function returns a
version below 1. DDL and the registration helper are in
`tests/test_measurement_content_write_barrier_spike.py`
(`BARRIER_TRIGGERS_DDL`, `register_measurement_contract`); Stage 3 moves them
into `init_reference_library_schema` (triggers created after
`_ensure_restrict_foreign_keys`, since a rebuild drops them) and
`register_measurement_contract` into the connection factories below.

**Why it is enforceable.** SQLite compiles trigger bodies into the DML
statement at prepare time and resolves functions then. A connection without
the function fails with `OperationalError: no such function:
sporely_measurement_contract` before any row is touched; there is no code path
in an older binary that could register it. Proven by the spike, which builds
the real schema through `init_reference_library_schema` in a temporary
database:

| Case | Test | Result |
| --- | --- | --- |
| (a) unaware bound / raw_text / legacy-id UPDATE on enhanced row | `test_unaware_update_of_enhanced_row_fails_closed` | fails, row unchanged |
| (b) aware edit, clear, successor insert, upsert | `test_aware_connection_can_edit_clear_and_supersede`, `test_aware_upsert_mirrors_pull_write_domain` | succeeds |
| (c1) clearing extension fields from unaware connection | `test_unaware_cannot_bypass_by_clearing_extension_fields` | fails |
| (c2) attached-database import (`_merge_reference_entity` through `ATTACH ... AS portable_reference`, as in `import_portable_payload`) | `test_unaware_attached_database_import_fails_and_aware_succeeds` | unaware fails, aware succeeds |
| (c3) legacy bundle merge (`_upsert_library_row_by_revision`, as in `import_database_bundle`) | `test_unaware_bundle_import_merge_fails_and_aware_reproduces_current_behavior` | unaware fails |
| (c4) pull-style upsert `INSERT … ON CONFLICT DO UPDATE` (`_write_domain`) | `test_unaware_upsert_as_used_by_pull_reconciliation_fails` | fails |
| (c5) successor of an enhanced row from an unaware `create_revision`-style INSERT | `test_unaware_successor_of_enhanced_row_cannot_be_created` | fails |
| (c6) old-binary table rebuild (`_rebuild_table_with_restrict_fks` with a DDL lacking the columns) | `test_old_binary_table_rebuild_fails_closed_and_keeps_the_barrier` | fails before DROP; columns, rows and triggers intact |
| trigger body actually runs (registered function returning 0) | `test_aware_connection_declaring_a_lower_contract_version_is_rejected_by_the_trigger_body` | `IntegrityError: measurement content contract required` |
| older startup `init_reference_library_schema` on an enhanced library | `test_unaware_reads_and_startup_initialization_still_work` | succeeds; reads work |

**Discovered property — the unsupported-open policy.** Because function
resolution precedes evaluation of the trigger's `WHEN` clause, an unaware
connection cannot prepare *any* INSERT or UPDATE on `reference_measurement_sets`
once the barrier exists, including edits of legacy-only rows
(`test_unaware_update_of_legacy_row_also_fails_this_is_the_unsupported_open_policy`).
This is accepted as the policy the plan asked for: **after a contract-aware
binary has opened a library, older binaries can read the library and delete
measurement sets, but cannot create or modify them.** Every other table is
unaffected, so older binaries still start, browse, plot, attach existing sets
and edit works and treatments. The `WHEN` clauses remain so that aware
connections pay nothing on legacy rows and so that a registered-but-older
contract version (function returning 0) is confined to enhanced rows.

Rejected finer alternative: a transaction-scoped marker row checked by the
trigger (`EXISTS (SELECT 1 FROM reference_contract_session)`) would let
unaware binaries edit legacy rows, but a marker left behind by any aware
writer that commits early disables the barrier for every connection.
Connection-scoped registration cannot leak.

**DELETE is deliberately outside the barrier.** Deletion removes the whole
record visibly and cannot leave legacy content standing without its
extension; local tombstones then push `{"id", "deleted": true}`, which
section 9 also exempts (`test_unaware_delete_is_permitted_as_a_lifecycle_operation`).

**Registration points (audit of every writer path).** Sources: grep of
`INSERT INTO`/`UPDATE` targets on `reference_measurement_sets` and of every
`sqlite3.connect`/`ATTACH` in `database/`, `utils/`, `ui/`.

| Writer path | Connection | Registration in Stage 3 |
| --- | --- | --- |
| `MeasurementSetRepository.create/update/create_revision/delete` | `reference_library._connect_reference` → `get_reference_connection` + `init_reference_library_schema` | inside `init_reference_library_schema` (first statement) and in `database/schema.py::get_reference_connection` |
| `reconcile_reference_library_feed` (`_write_domain`, `_reconcile_tombstone`) | `get_reference_connection` + init, then `ATTACH … AS observation_db` | same |
| `copy_curated_bundle_to_personal_library` | `get_reference_connection` + init | same |
| `import_database_bundle` → `_upsert_library_row_by_revision` | `get_reference_connection` + init | same |
| `import_portable_payload` → `_merge_reference_entity`, `_insert_row`, replay UPDATE of `legacy_reference_value_id` | raw `sqlite3.connect(destination_main_database)` + `ATTACH … AS portable_reference` (line 2503–2507); does **not** call init | explicit `register_measurement_contract(destination_main)` after `ATTACH` |
| `export_database_bundle` → `_copy_table_rows` | writes a fresh bundle DB whose tables come from `_copy_table_schema` (table SQL only, no triggers) | none needed; bundles carry columns, not triggers |
| `utils/archive/full_backup.py`, `full_restore.py` | file-level copy | none; triggers travel with the file |
| `database/schema.py::_migrate_reference_values`, `_migrate_reference_mounts_and_stains` | raw connect | none; they write only legacy `reference_values` |
| `ui/reference_entry_editor.py`, `ui/reference_library_manager_dialog.py` | via repository | none (no direct SQL; grep finds no `reference_measurement_sets` DML in `ui/`) |
| tests and tools writing rows directly | raw connect | must call `register_measurement_contract` |

**What remains open for a human.** The spike proves the mechanism against
connections that behave like older binaries. Running an actual shipped
desktop build against an enhanced library (startup, browse, edit attempt
error text, delete, bundle import into an *old-schema* destination) is
human-gated. One case the barrier cannot reach: an older binary importing an
enhanced bundle into a library that a contract-aware binary has **never**
opened has no trigger to hit; `_upsert_library_row_by_revision` intersects
columns and inserts legacy-only rows. Nothing existing is damaged, but the
imported content is lossy on that machine. Mitigation is product-level
(release notes / minimum version), not schema-level.

## 7. Snapshot version 2 (blocker 3)

**Shape.** Version 2 is version 1 (`build_observation_reference_snapshot`;
exact key set `_SNAPSHOT_KEYS` in `curated_reference_forks.py`) with:

- `schema_version: 2`;
- `measurements` gaining `q_core_min` and `q_core_max` (number or null),
  17 keys instead of 15;
- one new top-level key `measurement_details`: the decoded details object or
  null. It sits **outside** `measurements`, whose values stay numeric-or-null
  (cloud `reference_snapshot_valid` line 288 enforces that for v1).

The details object keeps its own `schema_version`; snapshot 2 may carry
details 1 or a preserved future version. Fixtures: `snapshot_v1_legacy_only.json`,
`snapshot_v2_enhanced.json`.

**Emit rule.** `build_observation_reference_snapshot` emits v1 for a
non-enhanced row and v2 for an enhanced row. Unchanged legacy content thus
keeps producing byte-identical v1 snapshots, so no existing attachment
becomes stale on upgrade (`refresh_snapshot` and `snapshot_status` compare via
`observation_snapshots_semantically_equal`).

**Size limits.** Whole snapshot ≤ 65536 bytes as today
(`reference_snapshot_valid` line 260; `_validate_snapshot` line 159);
`measurement_details` canonical encoding ≤ 4096 bytes
(`MEASUREMENT_DETAILS_MAX_BYTES`); `raw_points` limits unchanged.

**Projection rule** (`snapshot_semantic_projection`, replaces the
revision-only filter in `observation_snapshots_semantically_equal`,
lines 297–305): unsupported `schema_version` ⇒ not projectable (`None`;
never equal to anything; readers reject or hold). Supported ⇒ drop
`schema_version` and `reference_revision`; `measurements` gains
`q_core_min`/`q_core_max` = null if absent; `measurement_details` = null if
absent. Equality is equality of projections. Hence v1 ≡ v2-with-null-extension,
and v1 ≠ v2-with-statistics (`test_semantic_projection_rules`).

**Reader rollout order** (each step deployable alone; nothing later starts
before the previous is live):

1. **Desktop readers accept v2**: `stage_observation_reference_use_feed`
   (`!= 1` at line 102 → `not in {1, 2}`), `curated_reference_forks._validate_snapshot`
   (version-keyed exact key sets), `utils/archive/portable_import._validate_reference_snapshot`,
   `observation_snapshots_semantically_equal` via projection. UI readers
   (`ui/main_window.py::_format_reference_successor_review`, the detail
   builder near line 9721, `ui/reference_library_manager_dialog.py` near
   line 2442) use `dict.get` and tolerate v2 already; showing the details is
   Stage 4.
2. **Cloud readers/validators** (`sporely-web`, separate stage):
   `private.reference_snapshot_valid` version-keyed exact keys;
   `private.public_reference_snapshot` (migration `20260828172243`) rebuilds
   `measurements` from an explicit key list and would drop the extension, so it
   becomes version-aware; curated tables relax
   `CHECK (snapshot_schema_version = 1)` (migration `20260829141735` line 145) to
   `IN (1, 2)`; curation intake `candidate_json` (`20260829145939`) gains the
   three keys under `measurement_set`.
3. **Cloud extension columns and RPC guard** (section 9);
   `private.reference_canonical_snapshot` emits v2 when the row is enhanced.
4. **Desktop writers**: local columns + barrier (section 6), payload registries
   (section 4), `build_observation_reference_snapshot` v2 emission, import
   policy (section 8).
5. **Feature activation**: enhanced editing and enhanced attachments are
   enabled only after 1–4 are deployed. A desktop older than step 1 that pulls
   an account containing one v2 use fails the whole use feed with
   "unsupported snapshot schema" (current behaviour, never silent omission).
   Choosing the waiting period or a minimum-version gate is a human decision.

## 8. Import policy for omitted or NULL extension (blocker 4)

Applies to `utils/db_share.py::_upsert_library_row_by_revision`,
`utils/archive/portable_import.py::_merge_reference_entity`, the portable
replay check in `_merge_reference_graph` (line 1339 onward) and cloud pull.
Current behaviour that motivates the rule, recorded in the spike: an omitting
higher-revision row through either importer revision-upgrades the numeric
columns and leaves the newer descriptors standing
(`test_unaware_attached_database_import_fails_and_aware_succeeds`, aware
half; `test_unaware_bundle_import_merge_fails_and_aware_reproduces_current_behavior`).

Acknowledgement is decided from key presence **before** any normalization
(never default missing keys to NULL first), using `acknowledgement_state`:
`absent` (no extension key), `complete` (all three), `partial` (some). For
bundle and portable sources the keys are present iff the source table has the
columns (`SELECT *` → `dict(row)`), so an old exporter produces `absent` rows
and a new exporter `complete` rows; `partial` arises only from a damaged or
hand-edited source and is **rejected on every path before any other rule**:
bundle outcome `rejected_partial_extension`, portable
`PortableIdentityConflictError("incomplete measurement content extension")`,
including the no-destination insert case. It is never downgraded to `absent`
and never treated as `complete`. "R omits" below means `absent`;
"R acknowledges" means `complete`.

| Incoming row R vs destination D (same id) | Decision |
| --- | --- |
| R partial (any D, or no D) | **Reject explicitly**; nothing written. |
| No D | Insert. Acknowledging R with content: `validate_measurement_content(..., mode="authoritative")`, reject the row explicitly on failure. Omitting R: legacy-only row, extension NULL. |
| `R.revision < D.revision` | Skip as today (`skipped_stale` / `continue`). |
| Same revision, R acknowledges | Compare the full group with decoded JSON equality; equal ⇒ equivalent/skip; different ⇒ conflict (`PortableIdentityConflictError`; bundle reports a conflict outcome). |
| Same revision, R omits, D not enhanced | Omission compares as NULL (recognized historical baseline) ⇒ equivalent if the rest matches. |
| Same revision, R omits, D enhanced | Not equivalent. Portable: `PortableIdentityConflictError("source predates measurement content contract")`. Bundle: new outcome `skipped_unacknowledged`, counted and surfaced in the import report; never `skipped_same`. |
| `R.revision > D.revision`, R acknowledges | Full replacement of the group including the extension (explicit NULL clears) after validation. |
| `R.revision > D.revision`, R omits, D enhanced | **Reject explicitly.** Bundle: outcome `rejected_unacknowledged_extension`, surfaced; portable: `PortableIdentityConflictError`. No partial update. |
| `R.revision > D.revision`, R omits, D not enhanced | Replace as today. |

`_equivalent_rows` therefore stops intersecting keys for
`reference_measurement_sets`: the three extension keys are compared as
required keys with the rule above. Fixtures: `import_row_omitting_extension.json`
(revision 3, omits the keys, also changes a bound) and
`import_row_explicit_null_extension.json` (identical except the keys are
present with NULL) and `import_row_partial_extension.json` (carries the two
`q_core_*` keys but not `measurement_details_json`); `test_import_fixture_*`
pin the decisions.

Cloud pull: the remote row always acknowledges once step 3 of section 7 is
deployed; before that, `_payload_from_mapping` raises on the missing key and
`stage_reference_library_feed` rejects the feed. A remote NULL extension with
a higher revision against unchanged local enhanced content is an acknowledged
clear by an aware client (the RPC guard guarantees no other origin) and is
applied through the group rule of section 5. Curated fork copy: v1 bundle ⇒
legacy-only row; v2 with supported details ⇒ copy all three; v2 with an
unsupported details version ⇒ `CuratedReferenceError` raised by
`copy_curated_bundle_to_personal_library` (validated in `_validate_snapshot`),
never a degraded row; curated copy is excluded from the opaque-acceptance rule
of section 3 because it creates editable local content.

## 9. Cloud request-key presence and lifecycle rules

Authoritative implementation: `public.sync_reference_measurement_set_unthrottled`
(renamed from `sync_reference_measurement_set` by migration `20260830193144`
line 181; the public name is the rate-limit wrapper). It is patch-style
(`jsonb_populate_record(v_current, p_payload - 'deleted')`, line 616), so
omitted keys keep the current value and JSON null sets SQL NULL.

1. Aware writers send all three extension keys on every measurement-set
   mutation that carries content, including create, update, restore and
   successor creation. The adapter must **not** strip a NULL extension key the
   way `sync_measurement_set` strips `raw_points_json` on create
   (`utils/reference_cloud_adapter.py` lines 194–198).
2. Server: `enhanced(row)` := any extension column non-NULL.
3. Guard on existing rows, placed after the first `no_change` return
   (line 621) and before the CAS check (line 622): if `enhanced(v_current)`
   and the payload lacks any extension key and
   `to_jsonb(v_next) - {revision,row_version,created_at,updated_at,deleted_at}`
   is distinct from the same projection of `v_current` ⇒ return
   `invalid_payload`. Existing status: disposition `rejected` in
   `_STATUS_DISPOSITION`, terminal, no retry loop, and old clients parse it
   (an unknown status would raise `ReferenceCloudProtocolError` in
   `_parse_result`). Diagnostics use server logs; the envelope has no message
   field.
4. Guard on creation (`NOT FOUND` branch): if `v_supersedes` names an enhanced
   row and the payload lacks any extension key ⇒ `invalid_payload`. Creation
   without a predecessor accepts omitting payloads (legacy row).
5. Lifecycle exceptions: (i) the tombstone payload `{"id", "deleted": true}`
   built by `utils/reference_cloud_sync.py::_execute_tombstone` is exempt
   because it changes nothing but `deleted_at`; (ii) any
   payload whose only effect is on `deleted_at` (restore or delete with
   identical content) passes the content-diff test above; (iii) unchanged
   requests keep the `no_change` result.
6. Explicit JSON null means clear. The INSERT branch maps JSON null to SQL
   NULL explicitly for the jsonb column
   (`CASE WHEN jsonb_typeof(p_payload->'measurement_details_json') = 'null'
   THEN NULL ELSE … END`); `->>` casts already yield NULL for the two doubles.
7. Validation on the server is row-level, not JSON-only:
   `private.reference_measurement_content_valid(public.reference_measurement_sets)`
   takes the populated `v_next` record (both the create and update branches)
   and enforces sections 1 **and** 2 — details structure and enum values via
   the component `private.reference_measurement_details_valid(jsonb)`, plus
   descriptor/column consistency (a descriptor requires its complete ordered
   pair; explicit extremes enclose the core pair), scalar-mean/interval
   exclusivity per metric, `q_core_min <= q_core_max`, and the 4096-byte
   canonical size. Failure returns `invalid_payload`. The server accepts only
   `schema_version` values it knows (`1` at first deployment); a CHECK
   constraint bounds type (object or NULL) and size as defence in depth. A
   server at step ≤ 2 already rejects enhanced writes as `invalid_payload`
   through `reference_payload_has_unknown_keys` (line 573): no lossy fallback.
9. Partial acknowledgement: a payload carrying some but not all of the three
   extension keys returns `invalid_payload` before any other check, on create
   and on update, whatever the row's state (section 3, acknowledgement states).
8. Key presence acknowledges the contract; it is not authorization. No client
   version registry.

## 10. Writer / reader map

| Plan path | Concrete enforcing symbol(s) |
| --- | --- |
| Repository create/update | `MeasurementSetRepository._validate` → `validate_measurement_content(content_from_row(merged), mode="edit")`; `update` keeps merging `updates` into the existing row |
| Successor creation | `MeasurementSetRepository.create_revision` validates the merged base before `create` |
| Parser and both editors | `references/measurement_parser.py` emits `MeasurementContent`; `ui/reference_entry_editor.py` and `ui/reference_library_manager_dialog.py` call the edit operations of section 3 (Stage 2/4) |
| Bundle and portable import | `_upsert_library_row_by_revision`, `_merge_reference_entity`, `_equivalent_rows` per section 8 |
| Curated fork | `curated_reference_forks._validate_snapshot` (version-aware) and `copy_curated_bundle_to_personal_library` (copies or rejects) |
| Cloud pull / direct SQL upsert | `stage_reference_library_feed` validates content per row; `_write_domain` writes contract-encoded JSON |
| Reconciliation | `_reconcile_live` group rule (section 5) then `validate_measurement_content` |
| Snapshot build/refresh | `build_observation_reference_snapshot` (v1/v2 emit rule), `observation_snapshots_semantically_equal` via `snapshot_semantic_projection`; `adopt_successor`, `refresh_snapshot`, `_do_attach` unchanged callers |
| Cloud mutation | `sync_reference_measurement_set_unthrottled` guard (section 9), `reference_measurement_details_valid` |
| Unaware local binary | triggers `reference_measurement_content_guard_update/insert` + `register_measurement_contract` (section 6) |
| Use-feed reader | `stage_observation_reference_use_feed` accepts versions 1 and 2 |
| Public/share/curation builders | `private.reference_canonical_snapshot`, `private.public_reference_snapshot`, shared-contribution builder in `20260830183210` preserve v2 or reject |

## 11. Representative old-client matrix

Clients: **D0** desktop before this work; **D1** desktop with snapshot
readers only (section 7 step 1; its payload column lists and repository are
still D0's); **D2** desktop with full support. Servers: **C0** before step 2
(no extension columns; unknown keys rejected); **C1** steps 2–3 deployed
(columns, row-level validation and RPC guard live; canonical snapshot emits
v2 for enhanced rows; enhanced editing not yet activated on clients); **C2**
C1 plus client activation (step 5).

A library is **upgraded** once any D2 has opened it (barrier installed);
otherwise **never-upgraded**. D0 and D1 behave identically except for the
use-feed row.

| Operation | D0 / D1 | D2 |
| --- | --- | --- |
| Open an upgraded library, browse, plot | works | works |
| Edit, create or supersede a measurement set in an upgraded library | blocked (`no such function`, section 6) | works |
| Edit, create or supersede in a never-upgraded library | **works with no protection**: `MeasurementSetRepository.update`/`create_revision` have no check; the local row is legacy content and diverges from any enhanced cloud/bundle original. Only the server guard catches it later. | works |
| Delete a measurement set locally | works | works |
| Edit works / treatments / attachments | works | works |
| Pull library from C1/C2 into a never-upgraded library | works; `_payload_from_mapping` keeps only the columns it knows, so the local row lacks the extension. It is **not** read-only: local edits succeed (row above) and the subsequent content push is rejected by the server (`invalid_payload`), leaving the item rejected locally. | works |
| Pull library from C1/C2 into an upgraded library | fails closed whenever the feed needs a measurement-set domain write: `_write_domain`'s `INSERT … ON CONFLICT DO UPDATE` cannot be prepared (`no such function`, spike case c4), the `BEGIN IMMEDIATE` transaction in `reconcile_reference_library_feed` rolls back, and the pull reports an error. Pulls that touch only works, treatments or already-identical sets still apply; nothing is written partially. | works |
| Pull library from C0 | works | feed rejected (`_payload_from_mapping` raises on the missing key); D2 ships only after C1 is live |
| Pull use feed containing a v2 use | D0: whole feed rejected. D1: works | works |
| Push content edit of an enhanced row to C1/C2 | `invalid_payload` (section 9 item 3) | works |
| Push delete of an enhanced row | works (lifecycle exemption) | works |
| Push enhanced content to C0 | — | `invalid_payload` (unknown keys, line 573); no fallback payload |
| Push enhanced content to C1 | — | accepted and stored (server support is dormant ahead of activation; activation is a client gate, not a server switch) |
| Import enhanced bundle into an upgraded library | blocked by barrier on UPDATE and INSERT | preserve or reject per section 8 |
| Import enhanced bundle into a never-upgraded library | lossy insert (no trigger present; section 6 open item) | n/a |

## 12. Acceptance cases deferred to Stage 2/3 (enumerated)

Each requires production code that does not exist yet; none is stubbed.

1. `validate_measurement_content` accepts `row_enhanced.json` and rejects each
   mutation in `test_invalid_mutations_of_the_example_are_rejected` with
   `MeasurementContentError`.
2. `decode_measurement_details` returns `UnsupportedMeasurementDetails` for
   `details_unsupported_future_version.json`; `encode_measurement_details`
   round-trips it unchanged; an edit operation on it raises.
3. `encode_measurement_details(hebeloma)` equals the stored text in
   `row_enhanced.json`; `measurement_details_equal` is true for re-ordered keys.
4. `MeasurementSetRepository.update` with `{"length_mean": 11.0}` on
   `row_enhanced.json` raises (exclusivity); clearing `mean_interval` first
   then setting the scalar succeeds.
5. `create_revision` of the enhanced row copies all three extension columns.
6. `_reconcile_live`: local bound edit vs remote tag edit on
   `row_enhanced.json` ⇒ conflict listing group fields; local `notes` edit vs
   remote tag edit ⇒ merge; identical group content on both sides ⇒ no conflict.
7. `_write_domain` stores `measurement_details_json` in canonical codec form.
8. `_upsert_library_row_by_revision(import_row_omitting_extension.json)` against
   the enhanced row returns `rejected_unacknowledged_extension` and changes
   nothing; `import_row_explicit_null_extension.json` returns `updated` with the
   extension cleared.
9. `_merge_reference_entity` with the omitting fixture raises
   `PortableIdentityConflictError`; with the explicit-NULL fixture it upgrades
   and clears; `_equivalent_rows` treats omitted-vs-NULL as equal only when the
   destination is not enhanced.
10. `build_observation_reference_snapshot(row_legacy_only)` equals
    `snapshot_v1_legacy_only.json` (given the fixture work/treatment) and
    `build_observation_reference_snapshot(row_enhanced)` equals
    `snapshot_v2_enhanced.json`.
11. `observation_snapshots_semantically_equal` on (v1, v2-null-extension) is
    true; on (v1, `snapshot_v2_enhanced.json`) false; on a version-3 snapshot
    false.
12. `stage_observation_reference_use_feed` accepts a use whose snapshot is
    `snapshot_v2_enhanced.json` and rejects version 3.
13. `_validate_snapshot` (curated) accepts `snapshot_v2_enhanced.json` and
    rejects it when `measurement_details` is moved inside `measurements`.
14. `copy_curated_bundle_to_personal_library` with a v2 bundle produces an
    enhanced row equal to `row_enhanced.json` in the group fields.
15. `init_reference_library_schema` twice on a legacy library adds the three
    columns and both triggers once; `register_measurement_contract` is invoked
    by every connection factory in section 6's table (test by writing an
    enhanced row through each production path).
16. Cloud (in `sporely-web`, pgTAP): omitting content mutation on an enhanced
    row ⇒ `invalid_payload`; `{"id","deleted":true}` ⇒ `updated`; explicit NULL
    ⇒ cleared; omitting successor of enhanced predecessor ⇒ `invalid_payload`;
    `reference_canonical_snapshot` emits v2 only for enhanced rows.
17. `import_row_partial_extension.json` is rejected by
    `_upsert_library_row_by_revision` (`rejected_partial_extension`), by
    `_merge_reference_entity` (`PortableIdentityConflictError`) and, on the
    cloud, with `invalid_payload`, both against `row_enhanced.json` and with no
    destination row; nothing is written.
18. `validate_measurement_content(content, mode="edit")` raises for
    `details_unsupported_future_version.json` while `mode="authoritative"`
    accepts it and `encode_measurement_details` returns its canonical
    re-encoding unchanged.

## 13. Decisions that amend the plan

- The barrier's coarse effect (section 6) replaces the plan's phrase
  "Unaware connections fail closed" for enhanced-row mutations with
  "unaware connections cannot INSERT or UPDATE the table at all"; the plan's
  handoff records this.
- `notes` is explicitly outside the conflict group (the plan's list said
  "raw text/points … data kind, character and measurement-method fields" and
  left notes unnamed).
- Rejected unaware cloud mutations use the existing `invalid_payload` status
  rather than a new status.
- Partial acknowledgement (some extension keys present) is a third state,
  rejected everywhere; the plan spoke only of omission versus explicit NULL.

## 14. Machine-readable specification

`tests/test_measurement_content_contract_fixtures.py` parses this block and
asserts its own constants against it, so prose and tests cannot drift apart
silently. Stage 2 loads the same block in a test of the real module.

```json contract-spec
{"details_schema_version": 1, "supported_details_versions": [1],
 "supported_snapshot_versions": [1, 2], "details_max_bytes": 4096, "snapshot_max_bytes": 65536,
 "metrics": ["length", "width", "q"],
 "metric_keys": ["outer_range", "core_range", "mean_interval", "median", "sd"],
 "outer_range_kinds": ["reported_extremes"],
 "core_range_kinds": ["unspecified", "typical_range", "reported_range", "percentile_interval"],
 "interval_kinds": ["reported_range", "typical_range"],
 "extension_fields": ["measurement_details_json", "q_core_min", "q_core_max"],
 "scientific_content_fields": ["character", "data_kind", "raw_text",
   "length_min", "length_core_min", "length_core_max", "length_max",
   "width_min", "width_core_min", "width_core_max", "width_max",
   "q_min", "q_core_min", "q_core_max", "q_max", "q_mean", "length_mean", "width_mean",
   "sample_size", "specimen_count", "mount_medium", "stain", "preparation", "measurement_method",
   "raw_points_json", "measurement_details_json"],
 "snapshot_v2_added_keys": ["measurement_details"],
 "measurements_v2_added_keys": ["q_core_min", "q_core_max"],
 "acknowledgement_states": ["absent", "complete", "partial"],
 "import_decisions": ["reject_partial_extension", "skip_stale", "skip_unacknowledged",
   "equivalent_if_content_equal", "reject_unacknowledged_extension", "replace"],
 "validation_modes": ["edit", "authoritative"], "cloud_rejection_status": "invalid_payload",
 "contract_function": "sporely_measurement_contract", "local_contract_version": 1,
 "barrier_message": "measurement content contract required",
 "barrier_triggers": ["reference_measurement_content_guard_update", "reference_measurement_content_guard_insert"]}
```
