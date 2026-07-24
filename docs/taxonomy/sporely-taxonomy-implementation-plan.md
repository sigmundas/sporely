# Sporely international taxonomy and release implementation plan

> Living implementation plan for agents and maintainers. Update this file as work progresses. This document defines stages, contracts, gates, evidence, rollback, and the procedure for publishing taxonomy with future Sporely releases. It intentionally contains no agent prompts.

## Document control

| Field | Value |
|---|---|
| Status | Stages 0–1 and Stage 2A complete; Stage 2B fixture work in progress |
| Created | 2026-07-22 |
| Primary implementation repository | `sporely-py` |
| Cloud schema and search repository | `sporely-web` |
| Other consumers | `sporely-landing`, future Sporely clients |
| Current bundled artifact | `database/reference_data/generated/vernacular_multilanguage.sqlite3` |
| Target backbone | Catalogue of Life Extended Release (COL XR), version-pinned per build |
| Scientific-name policy | One global accepted scientific taxonomy; no regional accepted-name overrides |
| Vernacular policy | User-selected language; retain all names and provenance within that language |
| Internal identity | Stable Sporely-assigned integer taxon ID; external IDs stored as text mappings |
| Cloud policy | Compact searchable fungal index, not a complete copy of build provenance |

### Status notation

- `[ ]` not started
- `[~]` in progress
- `[x]` complete and verified
- `[!]` blocked or requires a recorded decision
- `[—]` deliberately deferred or out of scope

An agent may mark a task complete only after adding the verification evidence requested by that task. Code existing is not sufficient by itself.

## 1. Outcome and non-negotiable principles

Sporely will use one global accepted scientific taxonomy for all users. Language selection controls vernacular names; country or region does not change the accepted scientific name. NorTaxa, Artportalen, iNaturalist, MycoBank, Index Fungorum, legacy GBIF, and future sources are mappings or enrichment sources, not competing primary keys.

The completed system must provide:

1. International fungal coverage without weakening Norwegian and Sámi search, Artsorakel resolution, or red-list linking.
2. Stable Sporely taxon identity when a source renames, moves, merges, or replaces a taxon.
3. Exact provenance for every source snapshot and every compiled artifact.
4. Search parity between desktop SQLite and the web cloud index for all data intentionally published to both.
5. A compact Supabase representation with deterministic replacement, validation, atomic activation, and rollback.
6. Reproducible application releases: the exact taxonomy artifact used by a Sporely release can be rebuilt and verified.
7. Safe handling of unresolved, provisional, split, merged, and ambiguous fungal taxa without silently forcing a match.

### Explicit non-goals for the first implementation

- Do not vary accepted scientific names by user country.
- Do not make iNaturalist the taxonomic authority.
- Do not use COL, GBIF, NorTaxa, or Artportalen identifiers as Sporely primary keys.
- Do not import all global animals and plants into Supabase.
- Do not make live external API availability a requirement for ordinary search or observation editing.
- Do not rewrite historical observation identifications when taxonomy changes.
- Do not redesign Artportalen fetching until its actual fetch and reconciliation scripts have been audited.
- Do not write agent prompts in this plan.

## 2. Confirmed current state

The attached source audit and scripts establish the following baseline:

- The current builder uses the numeric Darwin Core `id` field from NorTaxa as local `taxon_id`; it does not keep `id`, `taxonID`, and `acceptedNameUsageID` as distinct identifier namespaces throughout the final model.
- `rebuild_taxonomy_db.py` is deliberately offline and deterministic, but its post-build checks are mostly non-empty row counts and language presence.
- `build_unified_multilang_taxonomy_db.py` builds `taxon_min`, `vernacular_min`, `scientific_name_min`, and `taxon_external_id_min`; it stores external IDs as integers and permits the same external ID to map to more than one local taxon because uniqueness includes `taxon_id`.
- The current source precedence is NorTaxa accepted taxonomy, Artportalen Swedish overrides, then iNaturalist language enrichment.
- `update_inat_common_names.py` resumes by `scientificName`. An empty failed/not-found row is therefore treated as permanently complete, and request errors cannot be distinguished from true negative results.
- The current Supabase import is a lossy and append-like publication of SQLite: it omits scientific aliases, can duplicate vernacular rows, retains removed taxa, and can report completion after partial HTTP failures.
- The current local artifact contains approximately 112,777 taxa, 98,485 vernacular rows, 293,458 scientific names/synonyms, and 140,378 external IDs. These counts are a comparison baseline, not acceptance targets for the new global model.

### Required baseline evidence before implementation

- [x] Record the current Git commit for each consuming repository.
- [x] Record SHA-256 and byte size of the current SQLite artifact.
- [x] Export exact current table, index, and per-language counts.
- [—] Export the current Supabase taxonomy row counts, duplicate counts, relation sizes, and index sizes. Moved by recorded decision to the mandatory Stage 6 entry gate because public read-only access is RLS-limited and these measurements are needed for cloud schema/storage work, not Stage 1.
- [x] Capture the current `search_taxa` function definition and grants from the checked-in live-schema baseline; public RPC probes separately verified current observable behaviour.
- [x] Save a regression corpus of at least 100 searches covering accepted names, synonyms, vernacular names, diacritics, Sámi language codes, missing names, and external-ID resolution.
- [x] Include `Candolleomyces candolleanus`, `Psathyrella candolleana`, and `hvit sprøsopp` in that corpus.

Evidence location: `database/taxonomy/evidence/baseline/` (or the repository-equivalent path chosen in Stage 1).

## 3. Target architecture

### 3.1 Data flow

```text
Versioned COL XR snapshot ───────────────┐
                                         │
Versioned NorTaxa snapshot ── bridge ────┤
Artportalen cache ─────────── bridge ────┤
iNaturalist state ───────── names/IDs ───┤
Manual reviewed mappings ────────────────┤
                                         ▼
                           Sporely taxonomy compiler
                             │                    │
                             ▼                    ▼
                    Complete SQLite        Compact cloud export
                    + provenance           fungi/search subset
                             │                    │
                             ▼                    ▼
                    Desktop package        inactive Supabase slot
                                                  │
                                             validate/activate
```

### 3.2 Identity model

`taxon_id` is an immutable positive integer assigned by Sporely. It is never reused. A persistent compiler state database owns allocation and cross-release continuity.

External identifiers are text, even when a source currently uses digits. The identifier contract must distinguish at least:

```text
nortaxa_row_id              = Darwin Core id
nortaxa_taxon_id            = Darwin Core taxonID
nortaxa_accepted_usage_id   = Darwin Core acceptedNameUsageID
col_usage_id                = COL XR usage identifier
artsorakel_identifier_type  = exact identifier field returned by Artsorakel
```

No field may be generically labelled “Artsdatabanken taxon ID” in the compiler or documentation.

### 3.3 Core compiled schema

Exact SQL belongs to Stage 3, but the logical model is fixed:

#### `taxon`

One current Sporely concept row.

```text
taxon_id                 INTEGER PRIMARY KEY
accepted_usage_id        INTEGER NOT NULL
parent_taxon_id          INTEGER NULL
rank_id                  SMALLINT NOT NULL
is_extant                BOOLEAN/INTEGER
is_searchable            BOOLEAN/INTEGER
```

#### `taxon_usage`

One source's treatment of a name or concept. This preserves accepted names, synonyms, former accepted names, and provisional names without flattening source disagreements.

```text
usage_id                 INTEGER PRIMARY KEY
taxon_id                 INTEGER NOT NULL
source_id                SMALLINT NOT NULL
source_taxon_id          TEXT NOT NULL
scientific_name          TEXT NOT NULL
canonical_name           TEXT NOT NULL
authorship               TEXT NULL
rank_id                  SMALLINT
status_id                SMALLINT
accepted_source_taxon_id TEXT NULL
parent_source_taxon_id   TEXT NULL
is_source_preferred      BOOLEAN/INTEGER
source_release_id        INTEGER NOT NULL
```

Required uniqueness: `(source_id, source_release_id, source_taxon_id)`. A separate validation rule must reject one preferred source identifier mapping to multiple current Sporely taxa.

#### `taxon_mapping`

Cross-source or cross-release evidence. Relationship values are controlled: `exact`, `likely_exact`, `broader`, `narrower`, `overlapping`, `synonym`, and `unresolved`.

```text
from_usage_id
to_usage_id
relationship_id
confidence
method_id
review_status_id
evidence_json_or_reference
```

Only `exact` mappings that pass the configured confidence/review rule may automatically merge identities. Name equality alone is insufficient.

#### `taxon_name`

Scientific and vernacular search/display names in one normalized model.

```text
taxon_id
name_kind_id             # canonical, synonym, former accepted, vernacular, provisional
language_id              # NULL for scientific names
display_name
normalized_name
source_id
is_preferred
priority
```

Language is expressed with stable BCP 47-compatible codes, retaining distinctions such as `nb`, `nn`, `se`, `sma`, `smj`, `sv`, `es`, `en`, and `de`. Source geography may remain provenance but must not filter the main user language list.

#### `taxon_external_id`

```text
source_id
external_id              TEXT
taxon_id
id_role_id
is_preferred
source_release_id
```

Preferred current IDs must be unique per source and external ID. Historical/superseded mappings remain queryable but are explicitly role-labelled.

#### `source_dataset` and `source_release`

Store source code, dataset title, version, publication time, DOI/dataset key where available, download URL, license, archive hash, extraction counts, and imported time.

#### `taxonomy_release`

One row per compiled release with schema version, source-release references, compiler Git commit, build time, all artifact hashes, row counts, test result, and release status.

### 3.4 Observation identity contract

Every observation must retain:

```text
resolved_taxon_id
identified_usage_id or equivalent immutable source snapshot reference
identified_name
identified_source
identified_source_taxon_id
identified_source_release
identified_at
```

The current display may move from a former synonym to a new accepted COL name, but “identified as” remains historically exact. Unresolved/manual identifications remain valid records and can be mapped later.

### 3.5 Distribution scope

The first global core should include:

- all accepted fungal taxa in the pinned COL XR release;
- fungal synonyms and former accepted names needed for search;
- higher classification required to navigate and disambiguate fungi;
- Sporely-supported plant or other associated-organism taxa under an explicit scope policy;
- all NorTaxa taxa required for existing Norwegian functionality, even when not yet mapped to COL;
- provisional Sporely concepts for supported source taxa that cannot safely map to COL.

The exact inclusion rules must live in a version-controlled policy file, not be scattered through Python conditionals.

## 4. Repository and artifact layout

Stage 1 should establish one canonical layout. Recommended:

```text
database/taxonomy/
  README.md
  policies/
    scope.yml
    languages.yml
    source_priority.yml
    release_thresholds.yml
    manual_mappings.yml
  sources/
    col_xr/<version>/
    nortaxa/<version>/
  state/
    taxonomy_registry.sqlite3
    inaturalist_enrichment.sqlite3
    artportalen_cache.sqlite3
  generated/
    <release_id>/
      sporely_taxonomy.sqlite3
      cloud/
      manifest.json
      audit.json
      audit.md
      checksums.txt
  scripts/
  tests/fixtures/
  evidence/
```

Large upstream archives and generated databases should follow the repository's existing large-file/release-asset policy rather than being added to ordinary Git history. Manifests, policies, migrations, compact fixtures, and audit reports should be committed.

## 5. Implementation stages

## Stage 0 — Reproduce the Artsorakel failure and freeze the baseline

**Purpose:** Prove the immediate failure mechanism before changing the taxonomy model.

**Dependencies:** None.

### Tasks

- [x] Capture the sanitized raw Artsorakel response for the known failing example, including identifier value, JSON field name, returned scientific/former names, endpoint, and response date. Local observation 611 supplied the evidence; no image, location, or observation metadata was retained.
- [x] Establish whether Artsorakel returns NorTaxa DwC-A `id`, `taxonID`, another scientific-name ID, an API-specific ID, or a superseded identifier.
- [x] Query the current SQLite separately by `taxon_min.norwegian_taxon_id`, `taxon_external_id_min`, canonical scientific name, and scientific synonym.
- [x] Query the current Supabase RPC using the same ID and names. Direct table/catalog measurements remain blocked by RLS and available public permissions.
- [x] Classify the failure as one or more of: stale source, wrong identifier namespace, missing mapping, synonym absent from cloud, runtime lookup omission, or compiler defect.
- [x] Add regression tests for the proven identifier and fallback contract.
- [x] Implement no broad taxonomy changes in this stage. The added resolver is a pure, unwired contract module; production lookup behaviour was not changed.

### Required runtime fallback contract

For Artsorakel-derived identifications, the eventual lookup order is:

1. exact source + identifier namespace match;
2. exact returned accepted scientific name;
3. exact returned scientific synonym/former name;
4. preserve unresolved ID and name with a diagnostic state.

Never discard a usable returned name merely because direct ID resolution failed.

### Exit gate

- [x] Root cause documented with sanitized raw evidence.
- [x] Identifier namespace documented and tested.
- [x] Regression test exists.
- [x] Existing SQLite baseline and publicly observable cloud RPC baseline captured. Privileged cloud measurements are a mandatory Stage 6 entry gate under the 2026-07-23 decision.

### Progress update — 2026-07-23 — Stage 0 Artsorakel contract and baseline

- Status: [x]
- Scope completed: Reproduced observation 611 from its stored Artsorakel payload; proved the `scientific_name_id` contract; traced local build/import and cloud RPC paths; captured local SQLite and public cloud-RPC evidence; added isolated contract tests; froze and verified a 100-query corpus.
- Files changed: `docs/taxonomy/sporely-taxonomy-implementation-plan.md`; `database/taxonomy/evidence/baseline/{README.md,artsorakel-candolleomyces-sanitized.json,baseline_queries.sql,regression-corpus.json,stage0-baseline.json,validate_regression_corpus.py}`; `sporely-web/src/{artsorakel-taxonomy.js,artsorakel-taxonomy.test.js}`.
- Tests run and results: web `node --test src/artsorakel-taxonomy.test.js src/artsorakel.test.js` — 38 passed; desktop `./.venv/bin/pytest -q tests/test_taxon_lookup.py tests/test_vernacular_db.py tests/test_cloud_reference_taxon_lookup_integration.py tests/test_reference_values_taxon_lookup_integration.py` — 29 passed; corpus validator — 100 passed; JSON parse passed; validator syntax compile passed; SQLite `integrity_check=ok` and zero foreign-key violations.
- Artifact/manifests: `database/taxonomy/evidence/baseline/stage0-baseline.json`; sanitized response fixture `artsorakel-candolleomyces-sanitized.json`; 100-query corpus `regression-corpus.json`; reproducible SQL in `baseline_queries.sql`; validator `validate_regression_corpus.py`.
- Decisions made: Artsorakel `scientific_name_id` is recorded as the namespaced NBIC scientific-name identifier. `NBIC:54995` is not documented as interchangeable with NorTaxa `id`, `taxonID`, or `acceptedNameUsageID`; all three happen to equal `54995` in the captured stale source row.
- Contract implementation audit: `sporely-web/src/artsorakel-taxonomy.js` is imported only by its test and does not alter production resolution. It preserves the raw `NBIC:54995`, accepted name, and former name in unresolved results. The numeric component is passed only with the explicit `artsorakel` source and `nbic_scientific_name_id` namespace; it is never assumed to be a NorTaxa row ID.
- Demonstrated cause: the captured response supplies current accepted name `Candolleomyces candolleanus`, former/source name `Psathyrella candolleana`, and `NBIC:54995`. The bundled NorTaxa snapshot still treats `Psathyrella candolleana` as valid and lacks the current accepted name. The compiler stores DwC `id=54995` as local `taxon_id` and integer external ID, losing the `NBIC` namespace. The cloud importer omits `scientific_name_min`; `search_taxa` searches no external IDs or scientific aliases. Live RPC therefore misses the identifier/current name, while the former and vernacular names resolve with accumulated duplicates.
- Deviations from plan: `sporely-py` was unexpectedly on `main`, not `feat/taxonomy-v2-stage-0`; no branch switch was attempted because the plan directory was already untracked user work. No production runtime integration was made.
- Risks or blockers: Privileged cloud taxonomy row, duplicate, orphan, table-size, and index-size measurements remain unavailable. They do not block Stage 1, but are a hard Stage 6 entry gate.
- Next safe task: Begin Stage 1 contract and policy work. Do not begin Stage 6 without the privileged cloud measurements.

## Stage 1 — Freeze contracts, policies, and versioning

**Purpose:** Prevent schema implementation from silently making product-policy decisions.

**Dependencies:** Stage 0.

### Tasks

- [x] Add Architecture Decision Records for global COL XR authority, language-only presentation, internal integer IDs, compact cloud publication, observation history, and release compatibility.
- [x] Define `TAXONOMY_SCHEMA_VERSION` independently of content release IDs.
- [x] Define content release format as `tax-YYYY.MM.DD-NN`.
- [x] Define source release independence and immutable published artifact/manifests.
- [x] Create `scope.yml` for global fungi, continuity records, provisional records, independently scoped cloud publication, and an explicit unresolved associated-organism rule.
- [x] Create `languages.yml` with accepted language-code normalization and display order.
- [x] Create source and name priority policies. COL controls global accepted presentation; NorTaxa/Artportalen scientific names remain aliases; authoritative local vernacular names outrank iNaturalist duplicates within the same language.
- [x] Define mapping confidence and manual-review rules.
- [x] Define split, merge, rename, disappearance, identifier replacement, homonym, and provisional-taxon behaviour.
- [x] Define cloud inclusion rules independently of desktop inclusion rules.
- [x] Define release thresholds, severity vocabulary, hard failures, and named future baseline gates.
- [x] Define supported compatibility matrix fields for each app repository and audit current consumer assumptions.

### Identity continuity rules

- Same persistent COL usage ID: retain Sporely `taxon_id`.
- Accepted-name or parent change under the same COL ID: retain ID and record the change.
- Old accepted COL ID becomes a synonym of one new accepted ID with exact lineage evidence: normally retain the concept ID, but record the supersession.
- Split into multiple accepted taxa: retain the old concept as historical; allocate new IDs to children unless an explicit reviewed rule identifies one continuing concept. Never move all historical observations silently.
- Merge of multiple accepted taxa: create or select a current merged concept according to a reviewed mapping; retain all historical IDs and usages.
- Ambiguous name-only or fuzzy match: do not merge; create an unresolved mapping task.
- Deleted source record with no replacement: retain historical usage and mark inactive.
- Sporely IDs are never recycled.

### Exit gate

- [x] All policies are version-controlled, machine-valid, and reviewed for internal consistency.
- [x] No Stage 1 contract field or identifier namespace remains semantically ambiguous. Associated-organism product scope remains an explicit pending policy decision rather than an ambiguous schema field.
- [x] Compatibility/versioning contract is documented for all consumers.

### Progress update — 2026-07-23 — Stage 1 contracts and policies

- Status: [x]
- Scope completed: Added five ADRs; froze schema/content/source/app versioning; formalized identifier, identity-continuity, mapping, observation-history, scope, language, source/name-priority, release-threshold, manual-mapping, and consumer compatibility contracts; audited existing effective builder and consumer assumptions; added offline validation.
- Files changed: `database/taxonomy/README.md`; `database/taxonomy/policies/{scope,languages,source_priority,mapping_policy,release_thresholds,manual_mappings,release_contract}.yml`; `database/taxonomy/docs/{identity-contract,release-versioning,compatibility-contract,observation-identification}.md`; `database/taxonomy/validate_policies.py`; `database/taxonomy/tests/test_policy_validation.py`; `docs/architecture/decisions/0001` through `0005`; this plan.
- Tests run and results: `./.venv/bin/python database/taxonomy/validate_policies.py` — 7 policy files, 16 languages, 12 namespaces validated; combined policy and existing taxonomy tests — 36 passed in 5.66s; Stage 0 corpus validator — 100 queries passed; `py_compile` passed for the validator, validator tests, and corpus validator; secret scan returned no findings; no repository Markdown link checker is configured, so new local paths/links were manually reviewed.
- Artifact/manifests: Machine policies are JSON-compatible YAML and require no new parser dependency. `manual_mappings.yml` is valid and intentionally empty.
- Decisions made: `TAXONOMY_SCHEMA_VERSION=2`; releases use `tax-YYYY.MM.DD-NN`; `no` remains legacy/undetermined Norwegian rather than silently becoming `nb`; Sámi languages remain distinct; only evidence-qualified `exact` mappings can automatically share identity; full reconciliation stays in SQLite while cloud is a compact slice.
- Unresolved policy questions: Exact associated-organism/plant groups require an inventory of observation host/substrate and lookup workflows. Until approved, retain current supported non-fungal continuity records without expanding to all plants or animals. ASCII transliteration remains disabled/pending evidence.
- Deviations from plan: `sporely-py` remains unexpectedly on `main` rather than `feat/taxonomy-v2-stage-0`; no branch switch was attempted because Stage 0/plan files were existing untracked work. No consumer production code, schemas, databases, or source archives were changed.
- Risks or blockers: None for Stage 2. Privileged Supabase measurements remain the mandatory Stage 6 entry gate.
- Next safe task: Stage 2A, implement the offline-tested request/manifest contract and version-pinned `refresh_col_xr.py` acquisition path using fixtures first; do not download an archive until an explicit pinned release/dataset key is selected.

## Stage 2 — Versioned source acquisition and manifests

**Purpose:** Make every upstream input immutable, inspectable, and reproducible.

**Dependencies:** Stage 1.

### 2A. COL XR downloader

- [x] Implement `refresh_col_xr.py` using a pinned Catalogue of Life release, never an unrecorded “latest” during compilation.
- [x] Support full or taxonomically filtered download through ChecklistBank. Record the integer ChecklistBank dataset key, COL version, issued date, DOI, format, filter, requested fields, URL, and authentication requirement.
- [x] Prefer a filtered fungal/higher-classification archive when it is reproducible and materially smaller; preserve the exact download request definition in the manifest.
- [x] Validate archive checksum, expected files/metadata, required columns, encoding, declared record counts, and source license.
- [x] Retain the downloaded archive and extraction manifest immutably.

COL publishes monthly and annual Base and Extended releases. Monthly releases are retained in ChecklistBank for a limited period, while annual versions have stable long-term support. Therefore production manifests must record a resolvable dataset key and archive hash; never rely only on the word `latest`.

#### Fixture-first acquisition contract

- [x] Require and validate an explicit XR release label, integer ChecklistBank dataset key, issued date, archive format, scope/filter, fields, official endpoint, and expected license; reject `latest`, Base releases, ambiguity, secrets, and metadata mismatch.
- [x] Produce deterministic normalized request definitions and canonical request SHA-256 values independent of execution time, dictionary order, whitespace, and local paths.
- [x] Implement planned/downloaded/validated/failed manifest rules, injected streaming transport, incremental SHA-256, retained failure diagnostics, staging, and atomic promotion boundaries.
- [x] Implement safe immutable release paths, traversal rejection, identical-request idempotency, and different-request overwrite refusal.
- [x] Implement offline ColDP fixture ZIP validation for integrity, safe members, required metadata/table presence, and request metadata agreement.
- [x] Provide offline CLI commands for request validation/normalization, planning, fixture validation, and status. No live-download command exists.
- [x] Propose and metadata-verify an explicit real COL XR release and ChecklistBank dataset key; acquisition approval remains deliberately withheld.
- [x] Verify the selected export's exact API request, authentication, ColDP metadata/schema, fields, counts, license, and resolved URL against a real archive.
- [x] Perform and validate the first real archive download.

### Progress update — 2026-07-23 — Stage 2A fixture-first COL XR request and manifest

- Status: [~]
- Scope completed: Built the explicit pinned-XR request model, deterministic request identity, immutable local layout, manifest lifecycle, injected transport/staging boundary, safe synthetic ColDP ZIP inspection, dry-run/status CLI, and offline fixtures. No real network or archive acquisition occurred.
- Files changed: `database/taxonomy/scripts/{__init__.py,refresh_col_xr.py}`; `database/taxonomy/tests/test_col_xr_acquisition.py`; `database/taxonomy/tests/fixtures/col_xr/valid-request.json`; `database/taxonomy/README.md`; this plan.
- Tests run and results: new acquisition plus policy and existing taxonomy suites — 78 passed in 1.72s; policy validator — 7 files, 16 languages, 12 namespaces; Stage 0 corpus — 100 queries passed; Python syntax and fixture JSON parsing passed; secret scan found only the validator's forbidden-key literals, no secret values; no real `sources/col_xr` files exist.
- Official documentation consulted: COL release distinctions and retention (`https://www.catalogueoflife.org/building/releases`); archive formats, past releases, and custom filters (`https://www.catalogueoflife.org/data/download`); ChecklistBank integer dataset keys and custom-download authentication (`https://www.catalogueoflife.org/tools/api`); ColDP 1.2 ZIP, `metadata.yaml`, and tabular entity contract (`https://catalogueoflife.github.io/coldp/`).
- Decisions made: Requests may name official `ColDP`, `DwCA`, or `TextTree` formats, but this fixture boundary structurally validates only ColDP. Persisted endpoints must use an official HTTPS host and contain no secret query fields. No observed dataset key is a default.
- Deviations from plan: No metadata-discovery command was added because it is optional and would add a live API surface before the pinned acquisition contract is finalized.
- Unresolved format questions: Exact ChecklistBank export endpoint/method and authentication flow; selected ColDP schema/version; production metadata shape; fungal filter encoding and ancestor behavior; exact included fields; archive license expression; declared count locations; and whether a filtered export is reproducibly preferable to full XR.
- Explicit selection required: A maintainer must choose one published XR release, its integer ChecklistBank key, issued date/DOI, ColDP export definition, fungal-plus-ancestor filter, included fields, and expected license. The tool will not choose on the user's behalf.
- Next safe task: With that explicit selection approved, add an official metadata-discovery/verification adapter and captured sanitized response fixture, then lock the request schema to the selected ChecklistBank export API before enabling a real download command.

### Progress update — 2026-07-23 — Stage 2A metadata-only candidate verification

- Status: [~]
- Scope completed: Verified public release metadata for proposed `2026-07-17 XR` / dataset `315834` / DOI `10.48580/dgykv`; captured sanitized official fixtures and raw-response provenance; established exact accepted Fungi root usage `F` (kingdom, parent `CS5HF`) with ambiguity guards; implemented bounded injected-transport verification and acquisition rejection of unapproved proposals.
- Files changed: `database/taxonomy/scripts/col_xr_metadata.py`; `database/taxonomy/scripts/refresh_col_xr.py`; official and synthetic fixtures plus provenance under `database/taxonomy/tests/fixtures/col_xr/`; `database/taxonomy/tests/test_col_xr_metadata.py`; `database/taxonomy/col-xr-source-selection.proposal.json`; taxonomy README; this plan.
- Tests run and results: full Python suite — 959 passed, 1 skipped in 19.72s; focused acquisition/metadata/policy suite — 70 passed in 0.62s; Stage 0 corpus — 100 queries passed; policy validator — 7 files, 16 languages, 12 namespaces; touched Python syntax, seven JSON artifacts, and diff whitespace validated; persisted-artifact secret scan returned no findings.
- Export contract evidence: Official OpenAPI exposes JSON-object `ExportRequest` at `POST /dataset/{key}/export`, returning a UUID; job states are listed by `GET /export`; finished bytes resolve through `GET /export/{id}` as ZIP/octet-stream. Custom downloads require account authentication. Supported request formats and root/rank/synonym/extinct/classification options were recorded. No export endpoint was invoked.
- Fields and scope: Proposed minimum compiler fields are usage ID, scientific name, authorship, rank, status, parent ID, and accepted usage ID. Provenance/quality/source fields are audit-only. Media, treatments, types, interactions, distributions, and COL vernacular bulk are excluded from the proposed input.
- Decision: Recommend the full XR ColDP for the first acquisition because official material does not define whether a root-filtered archive includes ancestor usage rows or only classification data. This is a lineage-reliability decision, not a size estimate; archive size remains unmeasured.
- Artifact/manifests: `col-xr-source-selection.proposal.json` is machine-readable but has `approval_status: proposed` and `download_authorized: false`; the acquisition loader rejects it.
- Deviations from plan: None. Live work was limited to public GET metadata, exact taxon lookup, and official documentation/OpenAPI reads.
- Risks or blockers: OpenAPI does not document field-selection parameters, rate/poll cadence, job/archive retention, or filtered ancestor-row semantics. This custom-export analysis was subsequently superseded for the first full-release acquisition by the public prebuilt GET evidence below.
- Next safe task: Superseded by the public prebuilt delivery correction below; do not implement authenticated custom export for the first full XR acquisition.

### Progress update — 2026-07-23 — Stage 2A public prebuilt delivery correction

- Status: [~]
- Scope completed: Corrected the proposed first acquisition from authenticated custom `POST /dataset/{key}/export` job creation to the official pinned public prebuilt `GET /dataset/315834/export.zip?extended=true&format=ColDP`. The canonical identity remains the dataset endpoint and exact parameters; redirect targets are execution evidence only.
- Files changed: `database/taxonomy/col-xr-source-selection.proposal.json`; `database/taxonomy/scripts/{col_xr_delivery.py,refresh_col_xr.py}`; `database/taxonomy/tests/{test_col_xr_delivery.py,test_col_xr_metadata.py}`; `database/taxonomy/tests/fixtures/col_xr/official-public-download-head-315834.json`; taxonomy README; this plan.
- Tests run and results: full Python suite — 981 passed, 1 skipped in 14.39s; focused COL acquisition/metadata/delivery suite — 85 passed in 0.53s; Stage 0 corpus — 100 queries passed; policy validator — 7 files, 16 languages, 12 namespaces; touched Python syntax, eight JSON artifacts, secret scan, diff whitespace, and no-archive filesystem checks passed.
- Header evidence: HEAD returned one `302` to verified official host `download.checklistbank.org`, then `200 application/zip` with `Content-Length: 1383646570`, ETag `"5278c56a-65720b858a1cc"`, Last-Modified `Tue, 21 Jul 2026 15:31:43 GMT`, and byte-range support. No archive body was requested.
- Contract changes: Full-release delivery requires no authentication or export job. Redirects are limited to three and only `download.checklistbank.org` is permitted. Final ZIP/octet-stream types are allowlisted. Compiler-required/audit/ignored fields now describe local consumption only; acquisition preserves the complete immutable archive.
- Size and disk policy: Proposed maximum is 1.5 GiB (`1610612736` bytes), conservatively above the observed official HEAD length. Future approval must explicitly bind a maximum. Preflight reports expected and available bytes; streaming enforces maximum and declared length, hashes incrementally, uses staging, validates ZIP without full extraction, and retains original bytes/hash.
- Approval boundary: The proposal remains `proposed` and `download_authorized: false`. A future separately generated `col-xr-source-selection.approved.json` must bind exact proposal/request hashes, release identity, timestamp, canonical endpoint, maximum bytes, and redirect hosts. No approved artifact and no real-download command were created.
- Decisions retained: Full XR ColDP remains recommended because custom fungal-filter ancestor-row semantics are undocumented. The authenticated custom/partial export API remains documented but is not used by this proposal.
- Exit items intentionally open: Real archive download, full archive validation, exact internal schema/fields/counts/license, checksum, and immutable promotion.
- Next safe task: Maintainer review of the revised proposal and maximum. After explicit approval, add the fully tested approved-artifact-only streaming command before performing one bounded acquisition.

### Progress update — 2026-07-23 — Stage 2A failed-attempt repair and retry contract

- Status: [~]
- Outcome: Stage 2A acquisition remains incomplete. Attempt 1 consumed the original transfer authorization by opening the GET response, but wrote zero bytes and promoted no archive.
- Directory failures: Initial planning incorrectly required a future parent to exist. After nearest-existing-ancestor planning was added, streaming opened the response and then incorrectly attempted to recreate the already validated `.staging` directory. Both defects are now covered by offline regression tests.
- Ordering repair: Layout owns directory creation and device validation. Streaming requires a real non-symlink staging directory on the planned device, rejects an existing destination, and opens the partial file exclusively before invoking transport. Local path, permission, race, or exclusive-open errors therefore occur before network access.
- Evidence model: The failed manifest retains its original error and now has append-only attempt 1 evidence: transport attempted, response opened, zero bytes written, expected size 1,383,646,570, no partial removal was necessary, and failure phase/type/message/timestamps are explicit.
- Retry model: Existing failed releases are eligible only when managed identity/policy/evidence match exactly, state is `failed`, attempt 1 wrote zero bytes, staging is empty, and no archive/extracted/unknown payload exists. Attempt 2 requires a separate authorization bound to proposal/request/original-approval hashes, attempts 1→2, maximum two GET attempts, immutable endpoint/ceiling, reason, timestamp, and acknowledgement of response-opened/zero-byte attempt 1. No retry authorization was created.
- Offline status: `retry-status` is read-only and network-free. The real failed release currently reports eligible for separate retry authorization, no blockers, no unexpected paths, and required next authorization number 2.
- Memory safety: Replaced reachable acquisition `Path.read_bytes()` archive hashes with bounded 8 MiB chunk hashing. ZIP member integrity paths already stream bounded chunks.
- Tests run and results: focused COL XR acquisition/metadata/delivery repair suite — 111 passed in 0.64s; full Python suite — 1007 passed, 1 skipped in 20.69s; Stage 0 corpus — 100 queries passed; policy validator — 7 files, 16 languages, 12 namespaces; touched Python syntax, 12 JSON artifacts, secret scan, diff whitespace, Git-ignore rules, and source-byte absence checks passed. Repair tests use injected transports and explicitly prove local and retry-preflight failures make zero transport calls.
- Network scope: This repair task performed no HEAD, GET, range request, redirect, DNS lookup, or other network operation.
- Next safe task: Obtain an explicit retry-authorization artifact for attempt 2. Do not reuse the original approval or start network access without it.

### Progress update — 2026-07-23 — Stage 2A attempt-2 retention and remote ZIP audit tooling

- Status: [~]
- Attempt 2 evidence preserved: Retry authorization SHA-256 `db7cc9d2a7ef603ceaea912415b536b239c80dc84be18d3ef33387a395cbb9c2`; 1,383,646,570 bytes transferred; streaming SHA-256 `397d701c8eb269bf78d6ac7b03149915b0d9e2a2c18694be2c91445b807814f9`; final redirect under `download.checklistbank.org/job/e8/e8ce17c8-47c4-4b10-8316-7b699472c3b1.zip`.
- Structural outcome: Validation stopped because member count exceeded the unchanged 20,000-member ceiling. This is an unresolved policy limit, not a finding of corruption or malicious structure. The completed staging file was deleted by the old behavior; no archive or quarantine exists locally and none was fabricated.
- Retention correction: Interrupted/incomplete bytes remain removable. Exact-size, fully hashed downloads that later fail structural validation now move atomically to managed `.quarantine/archive.zip`, never active `archive.zip`. Quarantine metadata records bytes/hash/attempt/reason/validator/rule/time/classification. Payload bytes are ignored; reports/checksums remain trackable; overwrite and ambiguous payloads fail closed.
- State model: Explicit lifecycle outcomes now distinguish `transfer_failed`, `downloaded`, `quarantined`, `validated`, and `promoted`. Attempt history remains append-only. Exact quarantined bytes may be promoted only after size/hash and the approved validator pass.
- Policy evidence required: Reassessment must include member count, central-directory bytes, aggregate/largest sizes, ratios, path normalization/duplicates/traversal, Unix types, methods/encryption, filename lengths, and ColDP file families. The 20,000 ceiling remains unchanged pending inventory.
- Offline tooling: Added ordinary EOCD and ZIP64 discovery plus bounded central-directory parsing for exact count, sizes, flags/methods, types, path hazards, aggregates, largest/ratio lists, and groupings. Malformed/truncated records fail closed without data-member reads.
- Proposed range audit: Machine-readable plan permits at most 64 MiB response data across one 65,557-byte suffix, optional 56-byte ZIP64 metadata, and one exact central directory. A future executor must require `206`, valid `Content-Range`, unchanged ETag/Last-Modified/length, approved HTTPS hosts/redirects, reject `200` before body, and perform no data-member range, retry, resume, or full download. Execution remains unauthorized.
- Tests run and results: focused COL XR and remote-ZIP suite — 128 passed in 0.78s; full Python suite — 1024 passed, 1 skipped in 20.08s; Stage 0 corpus — 100 queries passed; policy validator — 7 files, 16 languages, 12 namespaces; touched Python syntax, 15 JSON artifacts, Git-ignore behavior, secret scan, diff whitespace, source-byte absence, and attempt-history evidence checks passed. Socket-blocked fixtures cover offline parser/planner isolation.
- Network scope: This task performed no HEAD, GET, range request, redirect, DNS lookup, or other network operation. No attempt-3 authorization was created.
- Next safe task: Obtain separate authorization for the bounded remote central-directory audit, then use its exact inventory to review—but not automatically relax—the 20,000-member policy.

### Progress update — 2026-07-23 — Stage 2A member policy v2 and attempt-3 authorization

- Status: [~]
- Policy change: Versioned ZIP member policy v2 changes 20,000 from a hard stop to an audit warning and sets 250,000 as the emergency rejection ceiling. Member count alone is not corruption evidence; every path, type, encryption, compression, offset, expansion, integrity, and ColDP check remains mandatory.
- Retention: Byte-complete structural or emergency-policy failures quarantine exact bytes under `.quarantine/archive.zip`; interrupted transfers remain removable. Active promotion remains validation-only.
- Range audit: The earlier remote range proposal remains unauthorized and will not be executed for this attempt.
- Attempt 3: A separate artifact binds release/dataset/endpoint, proposal/request/original approval/attempt-2 authorization hashes, prior attempt 2, authorized attempt 3, maximum three transfers, expected bytes/hash, redirect hosts, and member policy v2 thresholds.

### Progress update — 2026-07-23 — offline quarantine metadata inspection

- Status: [!]
- Immutable archive: The quarantined archive remains 1,383,646,570 bytes with SHA-256 `397d701c8eb269bf78d6ac7b03149915b0d9e2a2c18694be2c91445b807814f9`; active and partial archive paths remain absent.
- Inventory: The ZIP has exactly 21,100 regular deflated members and a 1,333,957-byte central directory. It exceeds the 20,000 warning threshold and passes the 250,000 emergency ceiling.
- Metadata finding: The unique root `metadata.yaml` is 120,654,270 bytes uncompressed and 5,360,209 bytes compressed (ratio 22.509247307334473), with CRC32 `54f3ef78` and uncompressed SHA-256 `ae02692eaf1364d2928736435caac655271d28ea50177d57c197d0fd9e137771`.
- Decision: The observed metadata exceeds the mandatory maximum 64 MiB COL XR override, so the override was not implemented. Per the safety gate, YAML parsing, remaining structural validation, and promotion stopped. The exact archive remains quarantined and Stage 2A is incomplete.
- Next safe offline task: Determine why the official export embeds 21,085 source YAML files and a 120.7 MB aggregate root metadata document, then propose a separately reviewed validation strategy without weakening the 64 MiB gate.

### Progress update — 2026-07-23 — bounded YAML policy and source reconciliation

- Status: [!]
- Superseding metadata policy: A separately approved policy permits at most 256 MiB only for the exact pinned COL XR proposal/request/release/endpoint and inspected root metadata hash. The general ceiling remains 5 MiB.
- Complete YAML validation: `CSafeLoader` event parsing consumed 120,654,270 bytes in 7,366 bounded 64 KiB reads without constructing a Python document graph. It observed 13,641,047 events, 11,403,508 nodes, depth 5, zero anchors, zero aliases, and valid final-document termination.
- Structure and identity: The root is a mapping identifying dataset `315834`, title `Catalogue of Life`, version `2026-07-17 XR`, issued `2026-07-17`, DOI `10.48580/dgykv`, and license `cc by`.
- Provenance reconciliation: The root contains exactly 21,085 source references with unique numeric identifiers, exactly matching all 21,085 `source/<identifier>.yaml` members. These are delivery provenance rather than compiler input. A deterministic 20-file sample (first, last, smallest, largest, and 16 hash-selected paths) passed complete bounded YAML parsing.
- Terminal structural finding: `NameUsage.tsv` is present and non-empty, but its official ColDP headers are namespace-qualified (`col:ID`, `col:parentID`, `col:status`, and similar). The current validator did not recognize `col:ID` as the required `ID`/`taxonID` field, so validation stopped and the archive remains quarantined.
- Next safe offline task: Add separately reviewed, allowlist-based normalization for the official `col:` namespace, with fixtures proving arbitrary or deceptive namespace prefixes remain rejected, then resume the full `NameUsage` scan from quarantine.

### Progress update — 2026-07-23 — pinned ChecklistBank header profile

- Status: [!]
- Descriptor evidence: The archive contains no `datapackage.json`, `meta.xml`, or other descriptor declaring `col:`. All observed primary TSVs use lowercase `col:` fields and the `clb:merged` extension. This is treated only as a pinned ChecklistBank export convention, not a universal ColDP namespace rule.
- Header policy: One entity-specific resolver accepts exact canonical terms and exact lowercase `col:` allowlisted terms only for profile `checklistbank-col-xr-2026-07-17`. It preserves original-to-normalized provenance, retains known opaque `clb:merged`, rejects generic/nested/confusable/encoded prefixes and all normalization collisions, and never transforms cell values.
- NameUsage pass: Header resolution succeeded for all 73 columns. A bounded streaming pass reached EOF across all 2,929,163,002 uncompressed bytes with SQLite-backed primary-ID tracking.
- Terminal finding: Strict `csv.reader` TSV quoting semantics classified 112 records as malformed (`'\t' expected after '"'` or `unexpected end of data`). Per the stop gate, the scan was not retried and delimiter semantics were not weakened. The archive remains quarantined.
- Evidence limitation: The streaming SHA-256 and row counters were maintained internally but the terminal exception occurred before report serialization. They are intentionally recorded as unavailable instead of repeating the full scan automatically.
- Next safe offline task: Inspect bounded byte context for the recorded malformed line samples and establish whether ChecklistBank TSV treats quote characters literally or uses RFC-style CSV quoting. Propose and test exact parsing semantics before any new full scan.

### Progress update — 2026-07-23 — literal ColDP TSV scan

- Status: [!]
- Parser correction: TSV parsing now splits only on physical tab bytes and removes exactly LF or CRLF record terminators. Quotes and unknown escapes remain literal. Recognized `\\t`, `\\n`, `\\r`, and `\\\\` escapes are decoded only in a separate semantic view; raw values and identifiers are preserved.
- Evidence durability: The completed scan report was atomically written before duplicate and semantic policy evaluation. This completed report is explicitly marked complete and must not be confused with partial evidence.
- Complete NameUsage evidence: 2,929,163,002 bytes reached EOF with ZIP CRC verified; SHA-256 `5b7d7ec383ad69b7dc9c959dadd866a2769ea2433cbcbe1ae30f4b7d9359bdd0`; 7,871,064 valid rows; zero blanks; zero duplicate primary IDs; zero self-parent references.
- Former CSV diagnostics: 111 former strict-CSV quote failures are valid literal TSV records with exactly 73 columns. Bounded evidence retains line, row hash, byte length, quote context, and column count.
- Semantic probes: Fungi `F` remains accepted kingdom under `CS5HF`; `Candolleomyces candolleanus` (`9Z2GC`) is accepted; `Psathyrella candolleana` (`4NDVN`) is a synonym pointing to `9Z2GC`.
- Terminal finding: One record at line 1,853,650 contains a UTF-8 BOM outside the first header token. This is a genuine violation of the approved BOM rule, so the archive remains quarantined and remaining-table validation did not continue.
- Next safe offline task: Inspect bounded evidence for line 1,853,650 and decide whether an embedded BOM is source corruption requiring rejection or a separately authorized, explicitly evidenced normalization case.

### Progress update — 2026-07-23 — BOM compatibility inspection

- Status: [!]
- Exact occurrence: NameUsage line 1,853,650 has raw-row SHA-256 `156d19f3c53506a4f145799ff6ab1a5664c92b62e19e59bf20bd9573a907f9b5` and 511 bytes. It contains two consecutive `EF BB BF` sequences at record-body offsets 224 and 227.
- Field location: Both occur in zero-based field 31 (`namePublishedInPage`), beginning five bytes into the scalar after `58, f`. They are neither at record start nor field start.
- Structural context: The raw and one-removal views both have 73 columns; preceding and following records also have 73 columns. The affected row identifies accepted species `Virpazaria stojaspali` (`5BK77`).
- Decision: The requested compatibility authority required exactly one BOM at record or field start. This row has two mid-value occurrences, so the conditions fail and no normalization rule, rescan, remaining-table validation, or promotion was performed.
- Next safe offline task: Treat the double mid-scalar BOM as source-data corruption unless a new, explicit policy defines how a page citation such as `58, f<FEFF><FEFF>igs 6–7` should be represented without altering immutable source evidence.

### Progress update — 2026-07-23 — Stage 2A validated promotion

- Status: [x]
- Approved correction: Machine-readable policy `source-corrections.json` records correction `col-xr-2026-07-17-nameusage-5BK77-page-double-bom-v1`. It is bound to archive, member, release, dataset, line, row, field, offsets, raw hashes, and semantic hash. Only the semantic non-identity page citation changes from `58, f<FEFF><FEFF>igs 6–7` to `58, figs 6–7`; raw evidence is unchanged.
- Final NameUsage scan: 2,929,163,002 bytes; SHA-256 `5b7d7ec383ad69b7dc9c959dadd866a2769ea2433cbcbe1ae30f4b7d9359bdd0`; 7,871,065 rows; 111 former CSV false positives accepted; one correction; two removed semantic BOM code points; zero unapproved BOMs, malformed rows, blank rows, duplicate primary IDs, or self-parent references.
- Semantic probes: Fungi `F` is the accepted kingdom under `CS5HF`; accepted `Candolleomyces candolleanus` is `9Z2GC`; synonym `Psathyrella candolleana` is `4NDVN` and targets `9Z2GC`.
- Structural result: Root metadata passed complete bounded event parsing; 21,085 source references reconciled to 21,085 provenance members; the deterministic 20-file source sample passed; all 21,100 archive members passed path/type/encryption/compression/size/collision and streaming CRC checks.
- Promotion: After validation and exit checks, `.quarantine/archive.zip` was atomically moved on device `16777232` to active `archive.zip`. Pre/post size remained 1,383,646,570 bytes and SHA-256 remained `397d701c8eb269bf78d6ac7b03149915b0d9e2a2c18694be2c91445b807814f9`.
- Exit checks: Focused correction/schema/TSV/COL/ZIP/YAML tests — 189 passed; full Python suite — 1,106 passed, 1 skipped; frozen corpus — 100 passed; policy validator — 7 files, 16 languages, 12 namespaces.
- Stage state: Stage 2A acquisition and immutable validation are complete. Stage 2 overall remains open for NorTaxa acquisition and source-delta work.
- Next safe Stage 2 subtask: Begin Stage 2B fixture-first NorTaxa downloader work without importing or compiling the promoted COL archive.

### 2B. NorTaxa downloader

- [~] Implement `refresh_nortaxa.py` to download the versioned IPT Darwin Core Archive once. The fixture-first offline request, planning, manifest, and validation boundary exists; live download remains deliberately absent.
- [x] Read `meta.xml` instead of assuming filenames.
- [x] Preserve and distinguish core row `id`, `taxonID`, `acceptedNameUsageID`, `parentNameUsageID`, extension `coreid`, and namespaced scientific-name IDs as text.
- [~] Validate required Taxon and VernacularName columns, counts, encoding, and identifier uniqueness within each namespace. Structural fixture validation is implemented; real-source verification is pending.
- [ ] Retain previous snapshot and manifest.

### Progress update — 2026-07-23 — Stage 2B fixture-first NorTaxa DwC-A

- Status: [~]. Stage 2B is not complete.
- Proposal: Nortaxa (Artsnavnebasen), resource `artsnavnebase`, version `1.284`, issued `2026-07-17`, versioned IPT Darwin Core Archive; proposed archive/EML/resource endpoints on `ipt.artsdatabanken.no`; dataset UUID `a6c6cead-b5ce-4a4e-8cf5-1542ba708dec`; published Taxon/VernacularName counts `229018`/`58773`; weekly frequency; expected CC-BY 4.0; proposed ceiling 67,108,864 bytes. Every network-derived value remains proposed/unverified.
- Authorization: `nortaxa-source-selection.proposal.json` is explicitly `proposed` with `download_authorized: false`. Canonical proposal SHA-256 is `e025d53350422d1590836ddc6383f5ed93665ba82ec48db1b3708f2e337a67e3`. No approval artifact exists.
- Request/layout: Canonical fixture request SHA-256 is `38091edd85d40172539d3086732de2569a00102ff5564c66c55efb59360e7392`. It includes the canonical proposal SHA-256, and the persisted request and manifest repeat both identities. `sources/nortaxa/1.284/` contains only the proposed request and planned manifest; archive/staging/quarantine/extracted bytes are ignored while JSON evidence stays trackable.
- Shared boundary: The registry is honestly limited to source-profile metadata. NorTaxa reuses Stage 2A canonical JSON/SHA-256, secret rejection, immutable-release error, Git provenance, atomic JSON writes, and safe ZIP-member helpers. Its bounded DwC-A streaming and semantic validation are source-specific under `nortaxa_dwca`; COL limits, corrections, namespaces, YAML and ColDP table behavior are not used.
- Fixture contract: The deterministic ZIP uses safe nonstandard nested filenames, reordered-column/tab variants in tests, comma and tab delimiters, multiple ignored headers, UTF-8 Norwegian/Sámi and escaped content, and final rows both with and without newline. Its Taxon rows contain accepted `Candolleomyces candolleanus`, synonym `Psathyrella candolleana`, explicit accepted linkage, distinct core row IDs/taxon IDs, and `NBIC:54995`; VernacularName includes `hvit sprøsopp`, Bokmål, Nynorsk, Sámi, and preferred/non-preferred rows. Distribution is recognized and structurally checked but is not a compiler requirement.
- Identifier boundary: core row ID is only the archive-local primary/link key; `dwc:taxonID`, accepted and parent usage references, extension `coreid`, and Artsorakel/NBIC scientific-name identifiers retain their raw strings and separate roles. `NBIC:54995` is never reduced to `54995`; no final Sporely mapping is decided.
- Validation boundary: safe root `meta.xml` drives locations, row types, encoding, delimiter, line terminator, quote character, ignored headers, link indexes, field indexes, original term URIs, and canonical local names. ZIP table streams are read in bounded chunks without complete byte/string/table materialization. Declared columns are separate from semantic per-row requirements: accepted roots and higher taxa may omit accepted usage, parent, family, genus, and epithet fields; synonyms require accepted targets; genus/species rows enforce rank-relevant values. Consistent unmapped physical columns are allowed, while every declared index must exist and physical width must remain stable. Validation also covers XML declarations/entities, paths, ZIP safety/CRC/methods/ratios/counts/sizes, required files/terms, field bounds/EOF, duplicate IDs, usage references, orphan links, and incremental counts.
- Repair update: Selection values are loaded from and checked against the proposal rather than embedded as Python constants. Endpoints reject duplicate query keys, fragments, credentials, unofficial hosts, and nonstandard ports. `plan` reports whether state was created or already idempotent; it no longer describes a state-writing operation as a dry run.
- Repair verification: focused NorTaxa tests 41 passed; NorTaxa plus shared COL acquisition tests 83 passed; taxonomy tests 258 passed; full Python suite 1,147 passed and 1 skipped; frozen corpus 100 passed; policy validator 7 files, 16 languages, and 12 namespaces. Syntax, fixture XML/JSON, deterministic fixture evidence, Git-ignore, whitespace, secret-literal review, and socket-blocked network-isolation checks passed.
- Archive/compiler boundary: acquisition retains immutable raw evidence and optional terms; it does not replace legacy `taxon.txt`/`vernacularname.txt`, reconcile identities, compile source rows, or build a database.
- Network state: No NorTaxa network request occurred; no real archive, EML, or resource metadata was downloaded. The promoted COL XR archive was neither opened nor parsed.
- Next safe task: separately authorize metadata-only verification of the proposed IPT endpoints and published values. Do not authorize or perform archive acquisition as part of that task.

### Progress update — 2026-07-23 — Stage 2B metadata-verification attempt 1

- Authorization consumed: exactly one bounded GET of the versioned resource page, one bounded GET of the versioned EML endpoint, and one bodyless HEAD of the versioned archive endpoint were performed. No archive GET, Range, retry, fallback, authentication, or external-link request occurred.
- Outcome: failed during offline resource-page parsing. The normal IPT page contained login-form markup, and the initial heuristic incorrectly classified the complete page as a login response.
- Evidence limitation: all three transports and response-policy checks had completed before parsing. Response bytes, hashes, byte counts, redirects, and HEAD headers were still held only in process memory; the parser exception terminated the process before persistence. They are unavailable and must not be reconstructed or invented. No official value is marked verified.
- Append-only record: `sources/nortaxa/1.284/metadata-verification-attempt-1.json`, canonical SHA-256 `9665bb1ed16958830304e753dfdb73829bc9383b45d67ee9bc4dc332c66e067a`.
- Offline repair: login detection now rejects a login form only when the selected title/resource identity is absent. Each response is now journaled before parsing, and a parse failure stops later operations. The added sequencing regression proves only the first GET occurs when resource parsing fails.
- Validation after repair: focused metadata tests 22 passed; metadata plus NorTaxa/shared acquisition tests 105 passed; taxonomy tests 280 passed; full Python suite 1,169 passed and 1 skipped; frozen corpus 100 passed; policy validation, syntax, fixture XML/JSON, contact/secret-literal review, whitespace, Git-ignore, manifest, filesystem, and attempt-hash checks passed.
- State: proposal/request identities are unchanged; manifest remains `planned` and unauthorized with empty acquisition attempts; no `metadata-verification.json`, sanitized official fixture, approval, archive, or validation artifact exists.
- Next safe task: obtain separate explicit authorization for metadata-verification attempt 2. Do not reuse or retry attempt 1 authorization.

### Progress update — 2026-07-24 — Stage 2B metadata-verification attempt 2

- Status: [!]. Stage 2B remains incomplete. Metadata verification did not succeed.
- Authorization consumed: exactly one separately authorized metadata-only attempt bound to proposal SHA-256 `e025d53350422d1590836ddc6383f5ed93665ba82ec48db1b3708f2e337a67e3`, canonical request SHA-256 `38091edd85d40172539d3086732de2569a00102ff5564c66c55efb59360e7392`, and attempt-1 record SHA-256 `9665bb1ed16958830304e753dfdb73829bc9383b45d67ee9bc4dc332c66e067a`.
- Network operations attempted: `GET https://ipt.artsdatabanken.no/resource?r=artsnavnebase&v=1.284` — HTTP 200, `text/html`, 196,861 bytes, no redirects, response SHA-256 `35501bb5f85f42672357bdf28efcf6f91142245508f3b65a786be776fc4aa067`; `GET https://ipt.artsdatabanken.no/eml.do?r=artsnavnebase&v=1.284` — HTTP 200, `text/xml`, 5,755 bytes, no redirects, response SHA-256 `98dab203fdd38e13b8ec81a0d4d37129a56b90dc99ed69824d18851a53a0e6e9`.
- Operation correctly skipped: the versioned archive `HEAD https://ipt.artsdatabanken.no/archive.do?r=artsnavnebase&v=1.284` was not attempted because sequencing aborted before the third operation. No archive GET, Range request, retry, authentication, or external-link request occurred. Cumulative response body volume was 202,616 bytes, within the 4 MiB metadata ceiling.
- Terminal finding: `parse_eml` rejected the response with `AcquisitionError: unsafe EML declaration`. The bounded pre-parse safety gate detects a `<!DOCTYPE`, `<!ENTITY`, `SYSTEM`, or `PUBLIC` token anywhere in the response body. Real ChecklistBank EML responses typically begin with an `<!DOCTYPE eml:eml …>` declaration; that is exactly the token the safety gate refuses to accept without a separately reviewed XML-safe-declaration policy.
- Gates passed: HTTPS/host/query policy for both requests; response-status 200; redirect count 0 within the ≤3 limit; content-type `text/html` for the resource GET and `text/xml` for the EML GET; cumulative-body budget within 4 MiB; each response was journaled to the attempt-2 record before parsing was attempted; parsing sequence aborted immediately after the EML gate raised.
- Gates unavailable: archive HEAD content-length, ETag, Last-Modified, Accept-Ranges, Content-Disposition, and content type; EML `title`, `alternateIdentifier`/`packageId`, `pubDate`, `organizationName`, `intellectualRights`, and `edition`; and the resource-page parsed values are not persisted. The two GET response bodies were retained in process memory only for the duration of `verify()` and discarded on failure; only their byte counts and SHA-256 values are persisted. These are unavailable and are not reconstructed.
- Evidence path: `database/taxonomy/sources/nortaxa/1.284/metadata-verification-attempt-2.json`, canonical SHA-256 `92ab2958c151eab417da4d29084682293002950bdc5a72b1c0caaf8a48c66ad9`. It is append-only alongside the unchanged attempt-1 record.
- Manifest and release-directory state: unchanged. `manifest.json` remains `state: planned`, `approval_status: proposed`, `download_authorized: false`, empty `execution_attempts`, `download: null`, `validation: null`. `request.json` is unchanged. `nortaxa-source-selection.proposal.json` is unchanged. No `metadata-verification.json`, sanitized official fixture, approval, archive, staging, quarantine, or extracted payload exists.
- Post-attempt validation: focused metadata plus shared acquisition suite — 105 passed; taxonomy tests — 280 passed; full Python suite — 1,171 passed, 1 skipped; frozen 100-query regression corpus — 100 passed; policy validator — 7 files, 16 languages, 12 namespaces; Python syntax, JSON, secret and contact-information scan, filesystem, attempt-1 immutability, and attempt-2 canonical-hash checks passed.
- Decisions withheld: no repair, no retry, no fallback endpoint, no weakening of the XML-declaration safety gate, no acquisition approval, no proposal/request modification, and no attempt-1 modification. The contract documents and ADRs were treated as read-only.
- Eligibility: the pinned NorTaxa 1.284 selection is not sufficiently verified to become eligible for a separately reviewed archive-download approval. Archive acquisition remains unauthorized.
- Next safe offline task: propose a separately reviewed XML-safe-declaration policy that permits a plain `<!DOCTYPE eml:eml …>` prolog while continuing to reject `<!ENTITY`, `SYSTEM`, `PUBLIC`, and other external-resource declarations, prove it with offline fixtures, and only afterwards seek explicit authorization for a metadata-verification attempt 3.

### Progress update — 2026-07-24 — Stage 2B offline repair: XML prolog policy and per-op journaling

- Status: [!]. Stage 2B remains incomplete. This is an offline implementation-and-test task; no network operation, attempt 3, acquisition approval, archive operation, production change, commit, or push occurred.
- XML declaration policy: `validate_xml_prolog` in `database/taxonomy/scripts/nortaxa_metadata.py` replaces the earlier substring gate with bounded (default 4 KiB), declaration-aware inspection of the XML prolog. Raw response bytes and their SHA-256 are not modified. The prolog window admits an optional UTF-8 BOM, at most one `<?xml version="1.0"|"1.1" [encoding="UTF-8"]?>` declaration (≤256 bytes), zero or more well-formed XML comments (≤4 KiB each, no embedded `--`), at most one `<!DOCTYPE eml:eml>` declaration (≤256 bytes, required whitespace after the keyword, no external identifier, no internal subset, only before the root element), and exactly one root element start `<eml:eml …>`.
- Explicit rejections: `SYSTEM`/`PUBLIC` external identifiers; internal subsets (`[`, `]`); `<!ENTITY`, `<!NOTATION`, `<!ATTLIST`, `<!ELEMENT`, `<![CDATA[`, `<![INCLUDE[`, `<![IGNORE[`; general and parameter entity declarations; multiple DOCTYPE declarations; wrong root name; DOCTYPE after the root element; DOCTYPE hidden after or inside comments; processing instructions other than the XML declaration; unterminated, malformed, or oversized declarations and comments; UTF-16 and UTF-32 BOMs; NUL bytes in the prolog; XInclude directives (`xi:include`) anywhere in the body; and any residual `<!ENTITY` or `<!DOCTYPE` after the root element (belt-and-braces post-prolog scan).
- External-resource resolution is prevented at three layers: (1) the prolog validator refuses every construct that would introduce an entity or external reference; (2) `parse_eml` uses `xml.etree.ElementTree.fromstring` whose expat backend does not fetch external DTDs or resolve external general entities by default; (3) a post-prolog byte scan rejects `<!ENTITY`, `<!DOCTYPE`, and `xi:include` anywhere after the root element start. No production dependency is added.
- Official DOCTYPE shape verification status: the official attempt-2 EML response body was not retained. Its exact DOCTYPE declaration is unavailable for offline proof and is not fabricated. Only clearly labelled synthetic fixtures are used (`database/taxonomy/tests/fixtures/nortaxa/synthetic-eml.xml` plus inline byte strings). Whether the real ChecklistBank EML matches this grammar is still an open question that only a separately authorized metadata attempt 3 could answer.
- Per-operation atomic parsed-result journaling: `verify()` now exposes an optional `journal_sink` callback and threads state through a three-operation state machine (`resource_page` → `eml` → `archive_head`). Each operation transitions through `pending`, `transport_succeeded` (transport evidence journaled), and either `parse_succeeded` (sanitized parsed result journaled) or `parse_failed`/`transport_failed` (error record journaled). Later operations are explicitly marked `skipped` with a reason, distinguishing absent from unavailable values. The next network operation begins only after the preceding parsed-result journal transition has been emitted. Verification-state schema version is `2`. Frozen operation names: `("resource_page", "eml", "archive_head")`. A final `metadata-verification.json` may only be emitted when `state["final"] is True`, i.e. after all three operations reach `parse_succeeded` and cross-source consistency succeeds; partial evidence cannot be mistaken for the completed artifact.
- Offline replay: `replay_journal_state(state)` returns a deterministic, network-free read-only summary. It never re-invokes parsers and rejects unsupported schema versions.
- Attempt immutability: attempts 1 and 2 are byte-identical under this repair, and a regression test compares their canonical SHA-256 values (`9665bb…67a` and `92ab29…6ad9`).
- Files changed: `database/taxonomy/scripts/nortaxa_metadata.py`; `database/taxonomy/tests/test_nortaxa_metadata.py`; `database/taxonomy/tests/fixtures/nortaxa/synthetic-eml.xml`; `database/taxonomy/README.md`; this plan. Contracts, ADRs, proposal, request, manifest, and attempts 1 and 2 are unchanged.
- Test additions and results: focused metadata tests grew to 67; taxonomy tests 325 passed; full Python suite 1,216 passed, 1 skipped; frozen 100-query corpus 100 passed; policy validator 7 files, 16 languages, 12 namespaces; Python syntax OK for all touched files; JSON, fixture XML parse, and secret/contact scan OK; network-blocked pytest autouse fixture confirms all new tests run without socket access; attempt-1 and attempt-2 canonical hashes verified in-test.
- Eligibility: the offline repair is ready for a separate audit. Metadata verification is still not complete. The pinned NorTaxa 1.284 selection is not yet eligible for a separately reviewed archive-download approval; attempt 3 is not authorized by this task.

### 2C. Source deltas

- [ ] Generate machine-readable and Markdown deltas for every source update.
- [ ] COL delta categories: new/removed usages, accepted↔synonym transitions, accepted ID replacement, parent/rank/status changes, accepted target changes, and names reappearing under different IDs.
- [ ] NorTaxa delta categories: the same identifier/lineage changes plus Norwegian/Sámi names added, removed, or reprioritized.
- [ ] Treat unexpected source shrinkage or malformed lineage as a hard failure.

### Exit gate

- [ ] A clean machine can reproduce the same extracted source bytes from a manifest.
- [ ] Re-running acquisition for the same version is idempotent.
- [ ] No source file is promoted after partial validation.
- [ ] Source delta fixtures cover rename, split, merge, deletion, and ID replacement.

## Stage 3 — Build compiler v2 and stable registry

**Purpose:** Replace the NorTaxa-keyed flattened builder with a source-aware global compiler.

**Dependencies:** Stages 1–2.

### 3A. Persistent registry

- [ ] Create `taxonomy_registry.sqlite3` as controlled compiler state.
- [ ] Allocate monotonically increasing Sporely taxon and usage IDs in transactions.
- [ ] Store every allocation, source mapping, first/last-seen release, active state, and review decision.
- [ ] Make a registry backup mandatory before a build that can allocate or reconcile IDs.
- [ ] Add deterministic export and audit so registry corruption or accidental reallocation is detectable.

### 3B. COL core import

- [ ] Import the pinned COL XR slice and classification.
- [ ] Normalize rank/status values through lookup tables without losing original values.
- [ ] Retain accepted names, synonyms, authorship, source IDs, accepted lineage, and parent lineage.
- [ ] Assign/reuse Sporely IDs only through the registry rules.
- [ ] Preserve COL XR quality/source flags where available for audit, even if not shipped to cloud.

### 3C. NorTaxa bridge

- [ ] Match NorTaxa to COL using explicit source mappings first, then accepted/synonym lineage and higher classification.
- [ ] Never accept a bare canonical-name match when homonymy or concept disagreement is possible.
- [ ] Store `id`, `taxonID`, and `acceptedNameUsageID` as separate external IDs/usages.
- [ ] Import Norwegian and Sámi vernacular names with original language and preferred flags.
- [ ] Import NorTaxa scientific names not preferred by COL as searchable aliases.
- [ ] Preserve unmapped supported NorTaxa taxa as provisional Sporely concepts and create review records.
- [ ] Attach Norwegian red-list data to the source usage/assessment model rather than treating it as a universal taxon property.

### 3D. Compiler determinism

- [ ] Split acquisition, reconciliation, compilation, audit, and promotion into separate commands/modules.
- [ ] Make ordinary compilation network-free.
- [ ] Given identical source bytes, policies, registry snapshot, and compiler commit, produce logically identical data and stable IDs.
- [ ] Canonicalize row ordering, timestamps, and SQLite pragmas sufficiently to support stable logical hashes. Byte-identical SQLite is desirable but not required if a canonical data hash is recorded.
- [ ] Fail on any source ID collision that maps one preferred ID to multiple current taxa.

### 3E. Migration from current SQLite

- [ ] Build a one-time mapping from every current local `taxon_id` to the new Sporely ID.
- [ ] Report one-to-one, many-to-one, one-to-many, and unresolved mappings separately.
- [ ] Because the application is not yet in public use, prefer a clean schema migration over permanent compatibility columns, but retain the mapping artifact for fixtures and future diagnostics.
- [ ] Update observation foreign keys and preserve identification snapshots in a transaction on a copy of a real database.
- [ ] Verify that no observation, image, measurement, or identification becomes orphaned.

### Exit gate

- [ ] Full compiler test suite passes.
- [ ] Two builds from the same inputs produce the same canonical data hash and IDs.
- [ ] Current regression corpus resolves as intended.
- [ ] Split/merge fixtures do not silently reassign historical observations.
- [ ] `PRAGMA integrity_check` and `PRAGMA foreign_key_check` pass.

## Stage 4 — Enrichment state and language layers

**Purpose:** Retain useful iNaturalist and Artportalen names/IDs without allowing transient failures or stale CSVs to corrupt releases.

**Dependencies:** Stage 3 core model. iNaturalist and Artportalen may be delivered as separate sub-stages.

### 4A. iNaturalist state redesign

- [ ] Replace the CSV-as-cache with `inaturalist_enrichment.sqlite3`; generate CSV only as an optional interchange/debug export.
- [ ] Key requests by stable source usage/Sporely taxon identity, not scientific name alone.
- [ ] Store `lookup_status`: `success`, `not_found`, `request_error`, `ambiguous`, `inactive_redirect`, or `manual`.
- [ ] Store lookup method, queried name/ID, matched term, iNaturalist ID, active flag, accepted redirected ID, fetched time, expiry time, source snapshot, last error, and response hash.
- [ ] Retry request errors automatically.
- [ ] Retry true negatives only after configured TTL.
- [ ] Refresh successes after a longer TTL or explicit request.
- [ ] For renamed taxa with known iNaturalist IDs, fetch by ID first and detect inactive/redirected taxa.
- [ ] Do not convert fuzzy first-result matches into exact mappings without review.
- [ ] Compact inactive source rows while retaining audit history/versioned backup.
- [ ] Import names by normalized language code; preserve source spelling and deduplicate case/Unicode-normalized equivalents by policy.

### 4B. Artportalen redesign

- [ ] First audit the actual genus fetcher and Swedish-only reconciler; attach findings to this stage.
- [ ] Use a state database keyed by genus plus source-taxa hash.
- [ ] Refresh only genera affected by source changes, previous errors, expiry, or explicit request.
- [ ] Replace one genus's cached result atomically instead of appending.
- [ ] Regenerate matched and Swedish-only views from current state.
- [ ] Compact rows no longer returned.
- [ ] Preserve Artportalen names as aliases and Swedish vernacular names; never override the COL global accepted scientific name.

### 4C. Language pack/export policy

- [ ] Produce per-language counts and duplicate/preference audits.
- [ ] Permit multiple names in one selected language; `is_preferred` affects ordering, not visibility.
- [ ] Keep bridge data independent of language installation. A user may need Artsorakel/NorTaxa mapping while displaying English names.
- [ ] Decide through measured artifact sizes whether desktop languages ship in one SQLite file or attachable per-language SQLite packs. Do not split merely for architectural elegance.

### Exit gate

- [ ] Failed lookups are distinguishable from true negative results.
- [ ] An unchanged weekly source release causes no unnecessary external requests.
- [ ] Language-name losses beyond thresholds fail the release audit.
- [ ] Artportalen cache claims are verified against the actual implementation.

## Stage 5 — SQLite v2 integration and desktop behaviour

**Purpose:** Make the compiled model usable, fast, updatable, and historically safe in `sporely-py`.

**Dependencies:** Stages 3–4 as needed for enabled languages.

### Tasks

- [ ] Add repository/API classes so UI code does not depend directly on physical taxonomy tables.
- [ ] Search canonical scientific names, synonyms/former names, vernacular names for the selected language, genus, and supported external IDs.
- [ ] Normalize at build time and query time using one tested implementation.
- [ ] Rank exact before prefix; canonical before synonym; selected-language vernacular according to policy; deduplicate results by `taxon_id`.
- [ ] Return `match_type` and `matched_name` while displaying the current global canonical name.
- [ ] Display historical “identified as” where it differs from the current name.
- [ ] Support unresolved/provisional/manual concepts.
- [ ] Add runtime database schema compatibility check with a clear upgrade error.
- [ ] Add application startup verification of taxonomy manifest/hash where practical.
- [ ] Benchmark startup, exact lookup, two-character prefix, synonym search, vernacular search, and external-ID lookup on supported hardware.
- [ ] Package the manifest with the bundled database.

### Performance gates

Set numerical targets from baseline measurements before implementation is approved. At minimum:

- p95 local exact and prefix searches must not regress materially from the current database;
- application startup must not scan the full taxonomy;
- all ordinary search plans must use intended indexes;
- artifact size change must be explained by table/index in the audit.

### Exit gate

- [ ] Desktop integration tests and migration tests pass.
- [ ] Search corpus reaches expected results and ordering.
- [ ] Packaging test proves the released app contains the manifest-matched database.

## Stage 6 — Compact Supabase taxonomy and atomic publication

**Purpose:** Replace the lossy accumulated cloud copy with a small, complete-for-search, versioned publication.

**Dependencies:** Stage 3 cloud export; Stage 5 establishes shared search semantics.

### Mandatory entry gate

- [!] Obtain authorized read-only production access sufficient to measure current taxonomy table row counts, exact/case-insensitive duplicates, duplicate preferred names, orphan rows, relation sizes, and index sizes, and to capture the live `search_taxa` definition, grants, owner, `SECURITY DEFINER` property, and effective `search_path`.
- [ ] Save those measurements under the baseline evidence directory with capture time and reproducible queries.
- [ ] Do not make cloud schema, storage-budget, cleanup, or slot-sizing decisions until these measurements are captured. Unavailable access blocks Stage 6, not Stage 1.

### 6A. Measure and clean current production

- [ ] Measure current table and index sizes using PostgreSQL relation-size functions.
- [ ] Count exact and case-insensitive vernacular duplicates, duplicate preferred rows, stale rows, and orphan rows.
- [ ] Back up the existing taxonomy tables and function definition before cleanup.
- [ ] Add an emergency migration to prevent further duplicate vernacular imports even if the full v2 work is not yet complete.
- [ ] Make the current importer raise on all permanent batch errors and fail the run if any batch is incomplete.
- [ ] Avoid repeated production imports with the existing script until these protections are deployed.

### 6B. Compact slot schema

Use two physical schemas, for example `taxonomy_a` and `taxonomy_b`; exactly one is active. Each slot contains:

```text
taxa
  taxon_id INTEGER PRIMARY KEY
  canonical_name TEXT NOT NULL
  genus TEXT
  family TEXT
  rank_id SMALLINT
  parent_taxon_id INTEGER

taxon_names
  taxon_id INTEGER
  name_kind_id SMALLINT
  language_id SMALLINT NULL
  display_name TEXT
  normalized_name TEXT
  priority SMALLINT

taxon_external_ids
  source_id SMALLINT
  external_id TEXT
  taxon_id INTEGER
  id_role_id SMALLINT

taxonomy_metadata
  one row: release ID, schema version, source versions, hashes, counts, imported time
```

Small shared lookup tables for languages, sources, ranks, statuses, and name kinds may live in a stable schema. Do not repeat textual source and language labels per name row.

Required constraints:

- unique normalized name per taxon, kind, and language;
- one current preferred external mapping per `(source_id, external_id)`;
- foreign keys validated before activation;
- no duplicate preferred name for a taxon/language/kind where policy permits only one;
- no source ID mapped to multiple current taxa.

Build prefix indexes after bulk import. Start with normalized B-tree prefix search; add trigram only after benchmark evidence justifies its storage cost.

### 6C. Cloud export scope

Initially publish:

- current accepted fungi and required higher classification;
- supported associated taxa under `scope.yml`;
- scientific synonyms/former accepted names needed for search;
- names for languages exposed in the web app;
- NorTaxa identifiers used by Artsorakel;
- only other external IDs used for lookup/import/linking.

Exclude full reconciliation evidence, raw source rows, historical notes, descriptions, and unused external identifiers. Those remain in build artifacts/SQLite.

### 6D. Import inactive slot

- [ ] Export deterministic CSV or PostgreSQL `COPY` files plus manifest from the compiler.
- [ ] Check projected temporary storage headroom before creating a second slot.
- [ ] Truncate/recreate only the inactive slot; never mutate the active slot during upload.
- [ ] Bulk-load tables, then create indexes.
- [ ] Retry transient transport errors with bounded backoff; permanent errors abort.
- [ ] Record per-file row count, byte count, and checksum.
- [ ] Support safe resume only when the same release ID and file checksum are present.

### 6E. Validate inactive slot

- [ ] Compare exact counts and hashes against the compiler manifest.
- [ ] Validate constraints and orphans.
- [ ] Run duplicate/preference/collision queries.
- [ ] Run query plans and latency benchmarks.
- [ ] Run named probes for canonical, synonym, vernacular, missing-language, no-vernacular, and Artsorakel ID cases.
- [ ] Run desktop/cloud parity tests over the shared published corpus.
- [ ] Measure total table and index bytes and compare with budget.

### 6F. Atomic activation

Keep `public.search_taxa` stable for clients. In one transaction:

1. replace its body (and any stable views) to reference the validated inactive schema;
2. update one `public.taxonomy_active_release` metadata row;
3. execute smoke probes inside the transaction where possible;
4. commit.

The function must set a safe explicit `search_path`, clamp `lim` to `1..50`, trim input, enforce the configured minimum length, escape or deliberately handle `%` and `_`, filter permitted languages, and retain the current grants. Return `match_type` and `matched_name` while deduplicating by `taxon_id`.

Keep the previous slot unchanged through a rollback window. Rollback is the inverse transaction pointing the function and metadata back to the previous slot.

### 6G. Retention

- Keep the active and immediately previous slot in Supabase.
- After the rollback window and successful app monitoring, clear the older inactive slot before the next publication.
- Retain manifests, audit reports, and export checksums outside Supabase for every released taxonomy.

### Exit gate

- [ ] No duplicate or stale accumulation occurs across two consecutive test publications.
- [ ] Forced upload failure cannot affect active search.
- [ ] Activation and rollback tests pass.
- [ ] Cloud search supports scientific synonyms and external IDs.
- [ ] Measured database footprint is within the approved budget.

## Stage 7 — Web, landing, and API integration

**Purpose:** Adopt the new stable search contract without coupling clients to physical storage.

**Dependencies:** Stage 6.

### Tasks

- [ ] Update shared client types for internal `taxon_id`, current canonical name, `match_type`, `matched_name`, optional language name, and source external IDs.
- [ ] Update Artsorakel handling to send the exact source and identifier namespace.
- [ ] Preserve unresolved Artsorakel ID/name snapshots rather than dropping an identification.
- [ ] Update autocomplete and result labels so synonym matches display, for example, “Current: Candolleomyces candolleanus — matched: Psathyrella candolleana”.
- [ ] Ensure language selection, not region, controls vernacular results.
- [ ] Add cache invalidation keyed by active taxonomy release ID.
- [ ] Add telemetry/diagnostics for unresolved IDs and zero-result exact names without logging sensitive observation content.
- [ ] Confirm `sporely-landing` either consumes the same RPC/model or records why it has a separate taxonomy dependency.
- [ ] Test old client against new backward-compatible RPC before releasing the new client.

### Optional online fallback — deferred until core publication is stable

- [—] A server-side COL/ChecklistBank match fallback may be added for names outside the published cloud slice.
- [—] It must pin the target COL dataset/release, supply rank/classification when available, accept only configured high-confidence matches, preserve raw match evidence, and never allow arbitrary public queries to grow permanent cloud tables without bounds.
- [—] Cache should be bounded and separable from the released taxonomy; reviewed entries can enter a later compiled release.

### Exit gate

- [ ] Old and new clients pass compatibility tests.
- [ ] Artsorakel ID and name fallback tests pass end to end.
- [ ] Language behaviour is identical across web and desktop for the shared corpus.

## Stage 8 — Release tooling and continuous verification

**Purpose:** Make taxonomy maintenance an ordinary controlled release process rather than a bespoke manual rebuild.

**Dependencies:** Stages 2–7.

### Tasks

- [ ] Add one offline orchestration entry point that consumes already acquired inputs and builds into a new release directory.
- [ ] Add separate acquisition commands; compilation must never download implicitly.
- [ ] Generate `manifest.json`, `audit.json`, human-readable `audit.md`, and `checksums.txt` every time.
- [ ] Add CI fixture builds for every compiler change.
- [ ] Add scheduled source-check workflow that reports new COL XR/NorTaxa releases but does not auto-activate production.
- [ ] Add release-candidate workflow requiring human approval after audit thresholds.
- [ ] Sign or otherwise integrity-protect released manifests/artifacts if the distribution channel supports it.
- [ ] Add rollback drills for SQLite packaging and Supabase slot activation.
- [ ] Update `reference_data/README.md` to use the real rebuild command and explain the v2 workflow.

### Exit gate

- [ ] A maintainer who did not write the compiler can execute a source update from documentation.
- [ ] A failed build/upload/activation leaves the currently released app and cloud taxonomy unchanged.
- [ ] Every released artifact can be traced to exact sources, policy files, registry snapshot, and Git commit.

## 6. Release manifest contract

Minimum fields:

```json
{
  "taxonomy_release_id": "tax-2026.07.17-01",
  "taxonomy_schema_version": 2,
  "compiler_git_commit": "...",
  "registry_input_sha256": "...",
  "policy_sha256": "...",
  "sources": {
    "col_xr": {
      "version": "2026-07-17 XR",
      "checklistbank_dataset_key": "315834",
      "doi": "10.48580/dgykv",
      "archive_sha256": "...",
      "download_filter_sha256": "..."
    },
    "nortaxa": {
      "version": "...",
      "published_at": "...",
      "archive_sha256": "..."
    },
    "inaturalist": {
      "state_sha256": "...",
      "cutoff_at": "..."
    },
    "artportalen": {
      "state_sha256": "...",
      "cutoff_at": "..."
    }
  },
  "artifacts": {
    "sqlite": {"sha256": "...", "bytes": 0},
    "cloud_export": {"sha256": "...", "bytes": 0}
  },
  "counts": {
    "taxa": 0,
    "usages": 0,
    "names": 0,
    "external_ids": 0,
    "unresolved_mappings": 0
  },
  "language_counts": {},
  "validation": {
    "status": "passed",
    "audit_sha256": "...",
    "approved_by": "...",
    "approved_at": "..."
  }
}
```

Do not hand-edit a manifest after generation. Corrections require a new release candidate.

## 7. Release validation and hard-failure rules

Every release candidate must perform:

### Structural checks

- SQLite integrity and foreign-key checks.
- PostgreSQL constraints and orphan checks in the inactive slot.
- Required table/index/schema-version checks.
- Exact manifest/file count and hash comparison.
- One accepted current scientific usage per active taxon.
- No recycled or duplicate Sporely IDs.

### Semantic checks

- No preferred source external ID maps to multiple active taxa.
- No accepted taxon lacks its canonical searchable name.
- Every synonym target exists or is explicitly unresolved.
- Every parent reference exists or is intentionally root/unresolved.
- No unreviewed ambiguous mapping is treated as exact.
- Selected language codes are valid under `languages.yml`.
- At most one preferred vernacular per taxon/language/source when the source claims one; multiple non-preferred names are allowed.

### Delta checks

Initially require explicit review for:

- any source-ID collision;
- any change to a manually reviewed mapping;
- any one-to-many concept change;
- unexpectedly large accepted-taxon removal;
- more than 1% loss of Norwegian/Sámi names;
- more than 5% loss in any enabled enrichment language;
- a material increase in unresolved mappings;
- artifact or cloud index growth above the configured budget;
- named regression probe failure.

Threshold values must move into `release_thresholds.yml` after the first measured v2 baseline. Agents may not weaken them merely to make a build pass; changes require rationale in the decision log.

### Search checks

- Exact canonical, synonym, vernacular, and external-ID resolution.
- Prefix search with diacritics and normalized transliteration policy.
- Language filtering and fallback.
- Homonym disambiguation.
- No duplicate taxon result from multiple matching names.
- Query limit/input hardening.
- Desktop/cloud parity for the published subset.

## 8. Routine taxonomy data update procedure

This procedure updates taxonomy content without necessarily releasing new application code.

### A. Prepare

- [ ] Confirm working trees and record compiler commit.
- [ ] Back up the stable ID registry and enrichment state.
- [ ] Select explicit COL XR and NorTaxa versions; do not compile from floating `latest`.
- [ ] Create a new candidate release ID and directory.

### B. Acquire and inspect sources

- [ ] Download immutable source archives and manifests.
- [ ] Verify checksums, licenses, required fields, and counts.
- [ ] Generate old/new source deltas.
- [ ] Stop for malformed lineage, surprising shrinkage, or identifier collision.

### C. Refresh only affected enrichment

- [ ] Use source deltas to select iNaturalist taxa and Artportalen genera.
- [ ] Retry errors according to status/TTL rules.
- [ ] Generate enrichment delta and unresolved report.
- [ ] Do not conceal unavailable external services by recording empty success rows.

### D. Compile candidate

- [ ] Build into a new release directory from pinned local inputs.
- [ ] Never overwrite the currently bundled database.
- [ ] Generate SQLite, cloud export, manifest, hashes, and audit.

### E. Validate and review

- [ ] Run structural, semantic, delta, regression, performance, and size checks.
- [ ] Review every hard-warning category and record acceptance/rejection.
- [ ] Rebuild from scratch once after all mappings/policies are final.

### F. Publish cloud candidate

- [ ] Load the inactive Supabase slot.
- [ ] Validate counts, constraints, probes, plans, latency, and size.
- [ ] Activate in one transaction.
- [ ] Run production smoke tests.
- [ ] Retain the previous slot for rollback.

### G. Publish desktop artifact when applicable

- [ ] Copy the exact validated SQLite and manifest into the desktop packaging location through the release tooling.
- [ ] Verify packaged bytes match the manifest.
- [ ] Commit the manifest/reference or attach the binary according to artifact policy.
- [ ] Update the compatibility matrix.

### H. Observe and close

- [ ] Monitor unresolved-ID and zero-result diagnostics during the rollback window.
- [ ] Roll back immediately for semantic corruption, widespread search regression, or identifier mismatch.
- [ ] After the window, clear the older inactive cloud slot before the next update.
- [ ] Record final status in this plan's release history.

## 9. Procedure for a new Sporely application release

Taxonomy schema migrations and app deployment must be ordered so old and new clients remain safe.

### Case 1 — App release with no taxonomy schema change

1. Select an already validated taxonomy content release compatible with the app's required schema version.
2. Record `taxonomy_schema_version`, minimum compatible taxonomy release if any, and bundled SQLite SHA-256 in the app release metadata.
3. Run app tests against both the selected local SQLite and active Supabase release.
4. Verify the packaged desktop artifact hash.
5. Release the app. A newer compatible cloud taxonomy may be activated independently later.

### Case 2 — Taxonomy content update only

1. Follow the routine taxonomy update procedure.
2. Do not release app code if public RPC and SQLite schema compatibility are unchanged.
3. Activate cloud content and/or ship the desktop artifact through the existing app update mechanism.
4. Record which app versions were tested with the new content release.

### Case 3 — Backward-compatible taxonomy schema/RPC change

1. Deploy additive database migrations and backward-compatible RPC fields first.
2. Publish and validate a taxonomy release using the new schema while the old client still works.
3. Activate the new cloud slot.
4. Release updated clients.
5. Monitor adoption and errors.
6. Remove compatibility fields only in a later separately planned breaking release.

### Case 4 — Breaking taxonomy schema change

1. Increment `TAXONOMY_SCHEMA_VERSION` and create a migration/rollback plan.
2. Build dual-read or dual-RPC compatibility if technically feasible.
3. Release app code capable of reading old and new schemas before activation.
4. Verify adoption threshold and rollback path.
5. Activate the new taxonomy.
6. Retain the old cloud slot and old desktop migration backup through the defined window.
7. Remove the old path only after telemetry and support policy permit it.

### Release checklist required in each app repository

- [ ] Required taxonomy schema version declared.
- [ ] Tested taxonomy release ID recorded.
- [ ] SQLite hash recorded for desktop bundle.
- [ ] Active cloud release ID captured at test time.
- [ ] Old/new RPC compatibility test passed.
- [ ] Observation identification migration test passed when relevant.
- [ ] Rollback command/procedure verified.
- [ ] Release notes state taxonomy changes that users can observe.

## 10. Rollback and recovery

### Source/build rollback

- Source snapshots are immutable; select the prior manifest and registry backup.
- Never “fix” an already published release directory in place.
- Build a new candidate after correcting code, policy, or mappings.

### Desktop rollback

- Before local migration, back up the user database and record schema/hash.
- Migration must be transactional or restore from backup on failure.
- Bundled-reference rollback means shipping the previous manifest-matched artifact; never mix old DB bytes with a new manifest.

### Supabase rollback

- Keep the prior physical slot untouched.
- In one transaction, point stable search functions/views and active metadata back to the prior slot.
- Run the named production probes immediately.
- Preserve the failed slot and logs until root cause is documented; then clear it before reuse.

### Registry recovery

- The stable-ID registry is critical state. Back it up before every allocating build.
- If a bad candidate allocates IDs but is never published, IDs may remain retired; do not reuse them.
- If registry and published artifact disagree, stop publication and reconcile from the last manifest-matched backup.

## 11. Agent working procedure

Every implementation agent must:

1. Read this entire plan and the files relevant to its assigned stage.
2. Work on one bounded stage or sub-stage at a time.
3. Preserve existing user changes and avoid unrelated refactors.
4. Add tests before marking behaviour complete.
5. Update task status, evidence, decisions, and deviations in this file in the same change set.
6. Record exact commands and results under the stage evidence section; do not paste massive logs.
7. Mark a task `[!]` rather than guessing when an identifier, mapping, or destructive production action is ambiguous.
8. Never activate production, delete a cloud slot, replace the bundled DB, or rewrite observation IDs without explicit release authorization.

### Stage progress record template

Copy under the relevant stage:

```markdown
### Progress update — YYYY-MM-DD — <agent/change>

- Status: [~]
- Scope completed:
- Files changed:
- Tests run and results:
- Artifact/manifests:
- Decisions made:
- Deviations from plan:
- Risks or blockers:
- Next safe task:
```

### Decision log

| Date | Decision | Rationale | Affected stages | Approved by |
|---|---|---|---|---|
| 2026-07-22 | One COL XR scientific backbone; language-selected vernacular presentation | Avoid regional scientific-name differences while supporting international users | All | User direction |
| 2026-07-22 | Internal compact integer IDs; external IDs as text | Stable observation identity and smaller local/cloud indexes | 1, 3, 6 | Plan baseline |
| 2026-07-22 | Compact Supabase search index with inactive-slot publication | Limit storage and eliminate partial/accumulated imports | 6, 8 | Plan baseline |
| 2026-07-23 | Move privileged live Supabase measurements from the Stage 0 exit gate to a mandatory Stage 6 entry gate | Public RPC evidence is sufficient to establish the Stage 0 failure; privileged relation/duplicate/size measurements are required for cloud schema and storage decisions and should not indefinitely block Stage 1 | 0, 1, 6 | User recommendation |
| 2026-07-23 | Keep `no` as legacy/undetermined Norwegian; never collapse it or Sámi codes into `nb` | Existing artifacts use `no`, while `nb`, `nn`, `se`, `sma`, and `smj` have distinct semantics that must survive normalization | 1, 3–8 | Stage 1 contract |
| 2026-07-23 | Preserve current supported non-fungal continuity records while associated-organism scope is reviewed | Current artifacts contain non-fungal taxa, but product evidence does not justify either importing all plants/animals or silently removing existing support | 1–3 | Stage 1 contract |
| 2026-07-23 | Use JSON-compatible YAML for Stage 1 machine policies | It is valid YAML and permits standard-library offline validation without adding a production dependency | 1–2 | Stage 1 implementation |
| 2026-07-23 | Propose the public prebuilt full `2026-07-17 XR` ColDP GET as the first compiler input, without authorizing acquisition | Public metadata pins dataset `315834` and Fungi root `F`; full XR avoids undocumented filtered-export ancestor behavior. HEAD verifies a 1,383,646,570-byte archive and supports a conservative proposed 1.5 GiB ceiling | 2A–3 | Proposed; maintainer approval required |

## 12. Suggested implementation batches

These are code-review boundaries, not prompts.

1. **Batch A — Evidence and contracts:** Stage 0 plus Stage 1 policy/ADR files.
2. **Batch B — Source acquisition:** COL XR and NorTaxa immutable downloaders, manifests, deltas, fixtures.
3. **Batch C — Compiler core:** registry, source schema, COL importer, deterministic build.
4. **Batch D — NorTaxa bridge:** identifier namespaces, mappings, Norwegian/Sámi names, Artsorakel regression.
5. **Batch E — Migration/runtime:** SQLite v2 migration, historical observation identity, desktop search.
6. **Batch F — iNaturalist state:** status-aware cache and language import.
7. **Batch G — Artportalen state:** only after fetcher audit.
8. **Batch H — Cloud foundation:** emergency duplicate protection, compact schemas, hardened RPC.
9. **Batch I — Cloud publisher:** inactive upload, validation, activation, rollback.
10. **Batch J — Client adoption:** web/landing compatibility and end-to-end tests.
11. **Batch K — Release automation:** manifests, CI, documentation, drills.

Do not combine Batches C through I into one pull request. The highest-risk identity, enrichment, and production-publication changes require independently reviewable evidence.

## 13. Open implementation decisions to resolve with evidence

These do not change the agreed product direction, but must be settled before their dependent stage exits:

- [~] Proposed the public prebuilt full ColDP GET for the first `2026-07-17 XR` acquisition to preserve complete lineage; HEAD size is verified, while explicit approval and real-archive validation remain required.
- [ ] Whether Sporely's desktop artifact ships language tables together or as attachable packs, based on measured size and operational complexity.
- [ ] Exact scope for plants/associated organisms currently used in Sporely.
- [ ] Canonical Unicode/diacritic normalization and transliteration rules for search.
- [ ] Whether a stable source usage ID is stored directly on observations or represented by a frozen identification record.
- [ ] Initial cloud byte budget and query-latency thresholds after the production baseline is measured.
- [ ] Retention duration for the previous Supabase slot.
- [ ] Whether COL monthly or annual releases are the default production cadence; all builds remain version-pinned either way.
- [ ] Distribution mechanism for taxonomy-only desktop updates, if they should ship between application binaries.

## 14. References reviewed for this plan

### Attached project material

- `Pasted markdown(33).md` — audit of the current local pipeline and proposed refresh workflow.
- `rebuild_taxonomy_db(2).py` — current offline orchestration and count-based sanity checks.
- `build_unified_multilang_taxonomy_db(3).py` — current NorTaxa-keyed schema, aliases, vernaculars, and external-ID merge.
- `update_inat_common_names(2).py` — current name-keyed iNaturalist enrichment and resume behaviour.

### Official upstream references

- Catalogue of Life download page: <https://www.catalogueoflife.org/data/download>
- Catalogue of Life release model: <https://www.catalogueoflife.org/building/releases>
- Catalogue of Life API/ChecklistBank access: <https://www.catalogueoflife.org/tools/api>
- NorTaxa IPT resource: <https://ipt.artsdatabanken.no/resource?r=artsnavnebase>
- Supabase database-size guidance: <https://supabase.com/docs/guides/platform/database-size>

## 15. Release history

| Taxonomy release | Schema | COL XR | NorTaxa | SQLite SHA-256 | Cloud status | App versions | Notes |
|---|---:|---|---|---|---|---|---|
| Current legacy baseline | 1 | None | Existing unversioned snapshot | To capture in Stage 0 | Active legacy | Current development builds | NorTaxa-based IDs; cloud is lossy |
