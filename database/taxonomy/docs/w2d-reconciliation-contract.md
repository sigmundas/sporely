# W2D historical reconciliation contract

Status: **contract for Stage W2D dry-run reconciliation.** Not a production migration.

This document is the lead-agent integration gate for W2D. Every W2D implementation
in either repository must conform to it.

Inputs referenced:

* Macrofungi release `tax-2026.08.01-01`, scope-manifest SHA-256
  `72758b2c574e8aea27432b6b55c62dfb6ad87f3fadc11ad1c892a61abf23ac4e`, 52 881 concepts.
* Desktop starting revision `be14e6e28fb00b79a0d05265ef8d6b7008b669bc`.
* Web starting revision `edf81ce9692fade57942caa73012260df358a147`.
* Prior Phase-A audit counts (337 / 227 / 87 / 23) — **not reproducible locally**;
  see `local-snapshot-input.md`.

## 1. Identity backbone

* Global backbone: Catalogue of Life XR.
* Stable identity: `sporely_taxon_id` (integer).
* Identity may be created only from an exact namespaced external mapping
  `(source_system, namespace, external_id)` that resolves to exactly one
  taxonomy-v2 concept, or from a legacy lookup chain that ends in such a mapping.
* Identity may **never** be created from scientific-name equality, vernacular-name
  equality, an unnamespaced integer, scope membership, Red-List membership, or
  `(rank, name)`.

## 2. Reconciliation input record

Each historical observation is normalised (read-only) into a
`ReconciliationInput` with:

```text
observation_id                       stable internal reference (no personal data)
signals[]                            zero or more RawSignal records
manual_identification_flag           bool
stored_scientific_name               text | null
stored_vernacular_name               text | null
stored_rank                          text | null
source_release_or_timestamp          text | null
```

A `RawSignal` is:

```text
kind                exact | text-only
source_system       normalised (see §3)
namespace           normalised (see §3)
external_id         string (as observed; no coercion beyond string cast)
origin_field        source column or payload path
raw_value           verbatim stored value
notes               optional
```

Signals are collected from **all** historical fields — see §3.
Text-only signals (name / vernacular / guess) are recorded but never automatically
resolve.

## 3. Namespace derivation

Field → normalised signal mapping, per repository. Any field not producing an
exact `(source_system, namespace, external_id)` triple is kept only for
candidate-generation and snapshot preservation.

### Desktop (`observations` in local SQLite)

| field | signal |
|---|---|
| `artsdata_id` (int) | `nortaxa:nortaxa_taxon_id:{value}` — namespace inferred by convention documented in `migrate_observations_sporely_id.py`. |
| `ai_selected_service='artsorakel'` + `ai_selected_taxon_id` | If value begins with `NBIC:` → `nortaxa:nortaxa_taxon_id:{suffix}`. Bare integer → **invalid_or_unnamespaced_identifier** unless the presence of `ai_selected_service='artsorakel'` is treated as `nortaxa:nortaxa_taxon_id:{value}`; the policy MUST document the choice and every signal MUST carry the exact rule id. |
| `ai_selected_service='inaturalist'` (or `'inat'` normalised to `'inaturalist'`) + `ai_selected_taxon_id` | `inaturalist:inaturalist_taxon_id:{value}`. |
| `inaturalist_taxon_id` (int) | `inaturalist:inaturalist_taxon_id:{value}`. |
| `artportalen_id` (int) | `artportalen:artportalen_taxon_id:{value}`. Per identity-contract, artportalen precision is name-usage — the engine may still resolve it when the registry contains an exact mapping; otherwise **unresolved_external_identifier**. |
| `inaturalist_id` (int, observation id) | preserved only. Never creates identity. |
| `mushroomobserver_id` (int, observation id) | preserved only. Never creates identity. |
| legacy `adb_taxon_id` (dropped column, may persist in old DBs) | `nortaxa:nortaxa_taxon_id:{value}` via legacy-lookup-chain — treated as Level 2. |
| `sporely_taxon_id` if already populated | Level 2: legacy-lookup-chain against the registry — the resolution must still be evidenced against tax-2026.08.01-01. |
| `genus`, `species`, `common_name`, `species_guess`, `ai_selected_scientific_name`, `scientific_name_snapshot`, `taxon_rank_snapshot` | text signals (Level 5 candidate only). |

### Web (`public.observations` + `public.observation_identifications`)

| field | signal |
|---|---|
| `observations.artsdata_id` (int) | `nortaxa:nortaxa_taxon_id:{value}`. |
| `observations.artportalen_id` (int) | `artportalen:artportalen_taxon_id:{value}`. |
| `observations.inaturalist_id` (int, observation) | preserved only. |
| `observations.mushroomobserver_id` (int, observation) | preserved only. |
| `observations.desktop_id` (int) | Level 2 legacy chain against desktop taxon_min. If unrecoverable → **unresolved_legacy_identifier**. |
| `observations.ai_selected_service` + `ai_selected_taxon_id` | Same as desktop. |
| `observation_identifications.service` + `top_taxon_id` | Same as desktop. |
| `observation_identifications.results[]` | for each row where the payload contains a namespaced provider taxon id, emit that signal; text-only fields → Level 5 candidate only. |
| `observations.genus/species/common_name/species_guess/ai_selected_scientific_name` and `observation_identifications.top_scientific_name/top_vernacular_name` | text signals only. |
| Provider-integer columns on `public.taxa` referenced from an observation's foreign taxon id | Level 2 legacy chain via `public.taxa`. |

**Normalisation rules (both repos):**

* `ai_selected_service` values are lowercased, `'inat'` → `'inaturalist'`,
  `'nbic'` → `'nortaxa'`.
* `ai_selected_taxon_id` values matching `^NBIC:(\d+)$` are rewritten to
  `nortaxa:nortaxa_taxon_id:<int>`. Bare integers under `service='artsorakel'`
  use `nortaxa:nortaxa_taxon_id:<int>` and record `rule_id=artsorakel_bare_int_v1`;
  any deviation must be another rule id.
* Null, empty, whitespace-only values produce no signal.
* Unknown service or ambiguous prefix → **invalid_or_unnamespaced_identifier**.

Namespaces are canonical strings, lowercase, colon-separated. The registry uses
`(source_system, namespace, external_id)` — never coerce numeric ids to
integers in the manifest (store as string).

## 4. Reconciliation outcome states

Exactly one primary state per observation:

```text
resolved_exact
resolved_exact_via_legacy_mapping
resolved_exact_via_synonym_relationship
ambiguous_multiple_candidates
conflicting_exact_evidence
unresolved_external_identifier
unresolved_legacy_identifier
manual_unresolved
no_identity_evidence
source_record_missing
invalid_or_unnamespaced_identifier
```

Definitions match Stage W2D §6 verbatim.

## 5. Resolution hierarchy

Applied per observation, in order. The first level that yields exactly one
taxonomy-v2 concept **and** does not contradict any other exact signal wins.

1. **Direct taxonomy-v2 mapping** — an `(source_system, namespace, external_id)`
   signal maps directly to exactly one accepted concept in
   `taxon_external_id.jsonl` of the pinned release.
2. **Legacy lookup chain** — a legacy Sporely / desktop / web-local id resolves
   via a documented mapping to an exact namespaced external id in the pinned
   release. Chain is recorded step by step.
3. **Pinned synonym / name-usage** — a signal points to a name-usage in the
   pinned source (COL XR / NorTaxa) that carries an accepted-name relationship
   to exactly one accepted concept. Only pinned relationships qualify.
4. **Trusted secondary provider mapping** — an iNaturalist / Artportalen /
   Artsorakel identifier that resolves through a mapping already present in
   `taxonomy/registry/canonical/` or `taxon_external_id.jsonl`.
5. **Candidate generation only** — scientific-name / rank / vernacular candidates.
   Recorded separately; never assign identity.
6. **Preserve unresolved** — no chain exists; return an unresolved state; keep
   all signals and text snapshots verbatim.

**Multi-signal handling:** collect the resolved concept from every exact signal.

* All agree → primary state = highest-priority chain used
  (`resolved_exact` if any Level 1 chain agreed, else `_via_legacy_mapping`,
  else `_via_synonym_relationship`, else `_via_trusted_secondary_provider`
  which is captured under `resolved_exact` with `resolution_method =
  trusted_secondary_provider_mapping`).
* Any disagreement → **conflicting_exact_evidence** with all candidates listed;
  no source priority is invented.
* Some resolve, others unknown-but-not-contradictory → resolved; unknown signals
  preserved in `unmapped_signals`.

## 6. Result record

```text
observation_id                          string
reconciliation_state                    (see §4)
resolved_sporely_taxon_id               integer | null
resolved_canonical_name                 string  | null   (accepted-name text at release)
resolved_rank                           string  | null
resolved_scope_state                    string  | null   (extension; see §6.1)
resolution_method                       enum   | null
    direct_taxonomy_v2_mapping | legacy_lookup_chain
    | pinned_synonym_relationship | trusted_secondary_provider_mapping
resolution_evidence                     array of chain step objects
original_legacy_taxon_id                string | null
original_scientific_name                string | null
original_vernacular_name                string | null
original_source_system                  string | null      (primary)
original_source_namespace               string | null      (primary)
original_external_id                    string | null      (primary)
signals_all                             array of RawSignal
unmapped_signals                        array of RawSignal
candidate_concepts                      array of Candidate
conflicting_concepts                    array (populated only for conflicting_exact_evidence)
missing_source_records                  array
review_reason                           string | null
migration_action                        (see §7)
```

Field ordering: §9 requires keys sorted lexicographically at every level of
the JSON manifest, which supersedes the listing order shown above. The
listing above documents the semantic set of fields, not the physical
key order.

### 6.1 Scope-state extension

Stage W2D implementation added the `resolved_scope_state` field to the
result record so that snapshot preservation (contract §7) is machine-
checkable without re-consulting the pinned release. Values mirror the
pinned release's `taxon.jsonl.scope_state` verbatim (`include`,
`required_ancestor`, or any future value). It is null when
`resolved_sporely_taxon_id` is null. Recording this value never broadens
the macrofungi cache; it only preserves the pinned release's judgement.

`resolved_sporely_taxon_id` MUST be null unless state ∈
{ `resolved_exact`, `resolved_exact_via_legacy_mapping`,
  `resolved_exact_via_synonym_relationship` }.

`candidate_concepts` MUST NOT contain the resolved concept in duplicate.

## 7. Migration action classes

Per resolved record:

```text
reuse_existing_registry_concept                (sporely_taxon_id already registered in taxonomy_v2 registry)
materialize_existing_taxonomy_v2_concept       (concept in pinned release; not yet in cloud registry)
retain_unresolved_without_registry_concept     (any unresolved / no-evidence state)
manual_review_required                         (ambiguous / conflicting / candidate-only)
```

A resolved concept **outside** the macrofungi cache is still materialised in the
sparse registry — the cache is not broadened, and the concept's scope state is
preserved verbatim from the pinned release.

## 8. Snapshot preservation

For every migrated identification snapshot, the following historical fields
are **immutable** post-migration:

```text
original scientific-name text
original vernacular-name text
original rank
original legacy id
original namespaced provider id
original provider payload (where retained)
original observation timestamp (where relevant)
```

Resolution attaches identity in a separate, mutable resolution record:

```text
resolution_state           mirrors reconciliation_state
resolved_sporely_taxon_id
resolution_evidence        chain steps
resolution_release         taxonomy release id
resolution_timestamp       optional — excluded from semantic hash
```

Rewriting an original snapshot's display fields at resolution time is
forbidden. The canonical current name is served separately by the registry.

## 9. Determinism

The reconciliation manifest:

* uses UTF-8 JSON, `\n` newlines, no trailing whitespace;
* keys sorted lexicographically at every level;
* arrays sorted by a documented key (records: `observation_id`; signals: tuple
  `(source_system, namespace, external_id)`; candidates: `sporely_taxon_id`);
* `null` and empty arrays retained explicitly (no field elision) except where
  the field is documented optional in §6;
* excludes machine paths, hostnames, wall-clock timestamps;
* includes the following provenance header:
  * `manifest_version`,
  * `policy_sha256` (hash of `reconciliation-policy.json`),
  * `taxonomy_release_id`,
  * `taxonomy_scope_manifest_sha256`,
  * `input_source_hash` (hash of the normalised input records; not raw private data).

The **semantic SHA-256** is the SHA-256 of the manifest body with any documented
non-semantic fields excluded (see `reconciliation-policy.json`
`semantic_hash_excludes`). The same inputs + policy + release must produce a
byte-identical semantic body across runs.

## 10. Prohibited automatic behaviours

The reconciliation engine MUST NOT:

* resolve identity from scientific-name or vernacular equality;
* prefer one exact provider over another to break a conflict;
* create registry rows outside of a disposable local schema;
* mutate observation records in either repo;
* connect to production Supabase;
* consult private data outside the read-only reconciliation input.

## 11. Cross-repository ownership

* Desktop (`sporely-py`) owns: reconciliation engine, policy YAML/JSON,
  reconciliation manifest generator, fixture-backed tests, snapshot-input contract.
* Web (`sporely-web`) owns: disposable-schema migration simulation extending
  `w2c_sparse_experiment`, apply-manifest test driver, idempotency /
  rollback / snapshot-preservation tests, cloud-side documentation.
* Reconciliation manifest is authored on desktop; consumed by web verbatim.

## 12. Change control

Any deviation from this contract during implementation requires an update to
this document + the corresponding evidence entry. Silent deviation is a
hard-fail for the W2D deliverable.
