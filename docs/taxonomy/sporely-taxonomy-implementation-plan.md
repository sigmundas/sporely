# Sporely Taxonomy v2 Integration — Working Plan

**Plan version:** 2026-08-01
**Current programme state:** W0–W2B evidence accepted; full-Fungi runtime scope rejected; W2C redefined; W3 blocked
**Desktop accepted implementation:** `04602bcbb578e6abefb91ab96e03abbb42c53c3a`
**Web documentation branch:** `docs/taxonomy-v2-global-macrofungi-correction`
**Primary objective:** Introduce stable Sporely taxonomy identity across desktop, Supabase and web without breaking the existing taxonomy path or silently binding ambiguous names.

## Status legend

| Marker       | Meaning                                                              |
| ------------ | -------------------------------------------------------------------- |
| **DONE**     | Implemented, reviewed and accepted                                   |
| **CLOSEOUT** | Engineering accepted; repository or operational housekeeping remains |
| **NEXT**     | Next engineering stage                                               |
| **PLANNED**  | Defined but not started                                              |
| **GATE**     | Must pass before the following stage                                 |
| **BLOCKED**  | Must not proceed until the named issue is resolved                   |

---

## 1. Purpose and agent usage

This document is the programme-level source of truth for the taxonomy-v2 integration. It is intended to remain usable when switching between coding agents.

Every agent must:

1. Read Sections 2–6 before changing code.
2. Read the complete section for the current stage.
3. Inspect the actual repository, active branch, migrations and tests before trusting a path or signature in this document.
4. Work on one stage only unless this plan explicitly combines stages.
5. Preserve every accepted architecture decision.
6. Stop rather than invent identity mappings, namespace semantics, release metadata or migration behavior.
7. End with a commit, pushed branch, test evidence and an explicit stage verdict.

This document records programme decisions. The repositories remain authoritative for implementation details that may have changed after this plan version.

---

## 2. Current status at a glance

| Stage                                                   | Status                                | Accepted result or next action                                                                                  |
| ------------------------------------------------------- | ------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| W0 — Cross-repository audit and contract closure        | **DONE**                              | Existing schemas, RPCs, sync behavior, identity vocabulary and migration risks audited                          |
| W1 — Model-neutral cloud exporter                       | **DONE**                              | Accepted at desktop commit `04602bc`; deterministic seven-file JSONL export                                     |
| W1 repository integration                               | **CLOSEOUT**                          | Merge the desktop feature branch into `integrate/taxonomy-media-2026-07-30`; do not commit the generated export |
| W2A — Additive Supabase schema, activation and search   | **DONE**                              | Accepted at web commit `d61f2f9`; Model B schema and fixture-tested RPCs added without client cutover           |
| W2B — Importer, full local load and capacity validation | **DONE**                              | Import experiment passed; its complete-Fungi production scope and W2A representation are rejected                |
| W2C — Macrofungi scope and sparse-registry design       | **NEXT**                              | Verify the reviewed macrofungi policy against pinned COL IDs and design the sparse cloud registry                 |
| W2D — Cloud implementation                              | **BLOCKED**                           | Await W2C scope measurements and sparse-registry contract                                                         |
| Publication and provenance                              | **BLOCKED for production activation** | Any future scoped release needs complete licence and publication metadata                                       |
| W3A — Observation identity schema                       | **PLANNED**                           | Add stable concept identity, state and snapshots without removing legacy fields                                 |
| W3B — Sync, migration and backfill                      | **PLANNED**                           | Carry identity through desktop/web sync; backfill only from authoritative evidence                              |
| W4A — Web taxonomy service and picker                   | **PLANNED**                           | Introduce v2 search behind a flag; preserve manual and genus-only flows                                         |
| W4B — Artsorakel resolution and ambiguity UX            | **PLANNED**                           | Resolve namespaced service IDs; never identify by name fallback                                                 |
| W5 — Shadow validation, cutover and rollback            | **PLANNED**                           | Activate a release, compare old/new behavior and switch clients gradually                                       |
| Legacy retirement                                       | **PLANNED**                           | Remove old taxonomy only after the stability and rollback windows                                               |

The deployed web path remains:

```text
src/artsorakel.js
  → Supabase RPC search_taxa(q, lang, lim)
  → legacy public.taxa / public.taxa_vernacular
```

It remains untouched through W2A and W2B.

---

## 3. Non-negotiable architecture decisions

| Decision                   | Accepted rule                                                                                     | Consequence                                                                                                |
| -------------------------- | ------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Internal identity          | `sporely_taxon_id` is an immutable positive Sporely-owned integer                                 | External IDs must never be reinterpreted as Sporely IDs                                                    |
| Global canonical authority | COL XR is primary for canonical scientific presentation                                           | NorTaxa can enrich, bridge and provide national authority data, but does not silently replace COL identity |
| Cloud model                | External discovery plus a sparse Sporely registry and observation snapshots                       | Supabase must not contain the complete global fungal catalogue; W2A Model B remains experimental evidence  |
| Global product scope       | Reviewed global macrofungi                                                                         | Neither all Fungi nor any Norway-centred dataset defines the preload                                       |
| Desktop reference          | Reviewed global macrofungi subset, broader than the cloud registry                                 | Offline search may use a compact pack, but never the entire Fungi kingdom                                  |
| Plants                     | Outside taxonomy-v2 and the macrofungi search pack                                                  | Habitat plants/trees remain a separate product concern                                                     |
| W1 output                  | Model-neutral canonical JSONL                                                                     | The historical artifact remains unchanged; future scoped artifacts require new release identity            |
| Release behavior           | Multiple releases may coexist; exactly zero or one may be active                                  | A new release can be loaded and validated before transactional activation                                  |
| Ambiguity                  | Distinct Sporely IDs remain distinct even when names are identical                                | Search and resolver RPCs return ambiguity; they never collapse by spelling                                 |
| Genus-only                 | A selected genus concept is a valid resolved identification                                       | Non-null Sporely ID, rank `genus`, state `resolved`                                                        |
| Free text                  | Unmatched manual text is not a resolved concept                                                   | Null Sporely ID, with `manual` or `unresolved` state according to the interaction                          |
| Identity states            | `resolved`, `unresolved`, `provisional`, `manual`                                                 | Rank is separate from identity state                                                                       |
| Language                   | `nb`, `nn`, `no`, `se`, `sma`, `smj` remain distinct stored values                                | `no` is only a query umbrella for `nb`, `nn` and literal `no`                                              |
| Red List                   | Assessment data is enrichment, never identity evidence                                            | Red List rows cannot bind an observation to a concept                                                      |
| Authoritative IDs          | Only IDs with a preserved `(source_system, namespace, external_id)` contract may resolve identity | Namespace-lost integer rows are audit/legacy-only                                                          |
| Parent hierarchy           | Source `parent_taxon_id` is preserved even when its parent lies outside cloud scope               | No strict self-FK on release-scoped parent without an explicit W2 transformation                           |
| Compatibility              | All cloud changes are additive until W5 cutover succeeds                                          | Legacy tables, RPC and client behavior remain operational                                                  |
| Missing versus null        | Missing sync keys preserve existing cloud identity fields; explicit null clears them              | W3 tests must pin this distinction                                                                         |
| Generated data             | The complete W1 export is reproducible and is not committed to Git                                | An importer consumes an artifact verified through hashes                                                   |

No agent may reverse these decisions without a new explicit programme decision.

---

## 4. Repository and branch map

### Desktop repository

```text
Repository: sigmundas/sporely
Local path: /Users/sigmundas/Documents/Code/sporely/sporely-py
```

Relevant branches and commits:

```text
main
  802aed3  accepted taxonomy-v2 desktop code baseline

integrate/taxonomy-media-2026-07-30
  fe2c2e6  documentation closeout on top of 802aed3

feat/taxonomy-v2-w1-cloud-exporter
  f1a562a  initial W1 exporter
  2f04a43  provenance and validation corrections
  04602bc  final identity and output-validation closure
```

W1 should be merged into:

```text
integrate/taxonomy-media-2026-07-30
```

Do not merge or commit the generated cloud-export directory.

### Web and Supabase repository

```text
Repository: sigmundas/sporely-web
Local path: /Users/sigmundas/Documents/Code/sporely/sporely-web
```

Current W2 branch:

```text
feat/taxonomy-v2-w2-supabase-schema-search
```

It was created from `main`. It may carry W2A and W2B unless the import work becomes large enough to justify a follow-up branch.

Suggested later branches:

```text
feat/taxonomy-v2-w3-observation-identity
feat/taxonomy-v2-w4-web-picker
feat/taxonomy-v2-w5-cutover
```

Agents must not implement work in the wrong repository merely because both repositories are available in one workspace.

---

## 5. Accepted source artifacts and W1 contract

The release below is immutable evidence from the rejected broad-scope
experiment. Its scope predicate describes what W1 and W2B actually tested; it
is not the approved production scope and must not be relabelled as macrofungi.

### Release identity

```text
taxonomy_schema_version: 2
content_release_id: tax-2026.07.30-02
scope_predicate_id: fungi_closure_union_nortaxa_v1
COL XR release: 2026-07-17
ChecklistBank dataset: 315834
DOI: 10.48580/dgykv
NorTaxa release: 1.284
```

### Compiled source hashes

```text
SQLite SHA-256:
993d1608df1cb0ae93aab7b35a889b29858f0add720b933f3c58ed1e8485b94f

gzip SHA-256:
fb7660c613d0909c22591abe90768a9ae3c0ea88a8b8d5b2ee2bdf6c69cb8938
```

### Accepted W1 export hashes

```text
whole_export_sha256:
c3c770dca660b7995b3be253ba201bccd438e23a8aee3a7e06ed22659bf4a285

manifest_sha256:
096beb0b9363e69b31ced728d5ce55f7024e33c81b9a416ca1beefd0903e2d95
```

| File                                     |    Rows |       Bytes |
| ---------------------------------------- | ------: | ----------: |
| `taxonomy_release.jsonl`                 |       1 |       1,657 |
| `taxon.jsonl`                            | 634,894 | 325,471,016 |
| `scientific_name.jsonl`                  | 662,649 |  89,408,309 |
| `vernacular.jsonl`                       |  10,294 |   1,238,850 |
| `taxon_external_id.jsonl`                | 634,894 | 119,939,287 |
| `taxon_external_id_legacy_integer.jsonl` |  61,583 |  11,302,637 |
| `taxon_redlist.jsonl`                    |   7,866 |   4,552,259 |

Authoritative namespaces:

```text
col_xr / col_usage_id:          620,975
nortaxa / nortaxa_taxon_id:      13,919
```

Namespace-lost legacy rows:

```text
artsdatabanken source rows:       61,583
```

The legacy rows must never be used for automatic identity resolution.

### Scope consequence

The W1 export contains:

```text
634,894 of 634,895 concepts
```

This technically importable union of the COL Fungi closure and all NorTaxa is
rejected as the production runtime model. The complete pinned COL source
remains a build and reconciliation artifact for identity, synonyms, hierarchy,
stable source IDs, and release provenance. A future build must derive a
reviewed global macrofungi subset from it without using Norwegian occurrence,
NorTaxa membership, Norwegian or Sámi names, Norwegian Red List status,
Artsorakel coverage, or legacy Norwegian database membership as scope filters.

One excluded COL domain concept remains the parent of the exported Fungi kingdom:

```text
taxon_id:        152331
parent_taxon_id: 150361
name:            Fungi
```

W2 must either:

* preserve the dangling reference;
* treat Fungi as a root inside the cloud scope; or
* import the ancestor through an explicit documented transformation.

W1 does not repair or null the parent.

### Publication state

The current compiled release remains a candidate and is not marked published. Red List licence evidence remains unresolved.

Therefore:

* W2A schema and fixture work may proceed.
* W2B full local load and measurements may proceed.
* Production activation may not proceed until publication and provenance readiness is explicitly closed.

---

## 6. Programme stages and gates

The programme is staged as follows:

```text
W0  Audit and contract closure
 ↓
W1  Deterministic model-neutral exporter
 ↓
W2A Additive cloud schema and fixture-tested RPCs
 ↓
W2B Full importer and local size/performance validation
 ↓
Publication and capacity gates
 ↓
W3A Observation identity schema
 ↓
W3B Sync and authoritative backfill
 ↓
W4A Web taxonomy service and picker
 ↓
W4B Artsorakel and ambiguity UX
 ↓
W5  Shadow validation, activation and cutover
 ↓
Legacy retirement
```

A stage is accepted only when:

1. Its branch and commit exist.
2. Scope boundaries were respected.
3. Required tests ran.
4. Known unrelated failures were reported unchanged.
5. Data counts and hashes are exact where applicable.
6. Rollback or failure behavior is documented.
7. The final report ends with an explicit accepted/not-accepted verdict.

An agent must not begin the next stage merely because most of the current stage works.

---

## 7. W0 — Cross-repository audit and contract closure

**Status: DONE**

### Purpose

Establish the actual baseline before implementation:

* legacy Supabase taxonomy schema and `search_taxa`;
* web taxon object shapes;
* desktop taxonomy-v2 identity;
* observation persistence and sync behavior;
* migration and RLS conventions;
* importer constraints;
* storage risk;
* ambiguity and genus-only behavior.

### Accepted W0 results

* Legacy `search_taxa` does not provide a complete taxonomy-v2 identity contract.
* Web currently maps legacy search results directly in `src/artsorakel.js`.
* Existing observation payloads drop richer taxonomy-v2 identity before persistence.
* Desktop cloud sync intentionally firewalls the new observation identity fields until W3.
* The identity-state vocabulary is fixed as:
  `resolved`, `unresolved`, `provisional`, `manual`.
* Explicit genus selection is resolved identity.
* Free-text genus or species is not silently resolved.
* W1 must be model-neutral.
* W2 may use Model B.
* Full export mode is accepted; there is no W1 delta mode.
* PostgREST RPC must not be treated as a streaming `COPY FROM STDIN` importer.
* The Supabase baseline was 8/9 SQL tests, with one unrelated point-preparation failure.

W0 should not be reopened unless an implementation contradicts one of these accepted facts.

---

## 8. W1 — Model-neutral deterministic cloud exporter

**Status: DONE**

### Accepted implementation

```text
Branch:
feat/taxonomy-v2-w1-cloud-exporter

Accepted commit:
04602bcbb578e6abefb91ab96e03abbb42c53c3a
```

### Delivered behavior

The exporter:

* verifies the source gzip and decompressed SQLite hashes;
* verifies schema version and release identity;
* computes `Fungi closure ∪ all NorTaxa`;
* streams compiler-shaped rows to canonical JSONL;
* separates authoritative IDs from namespace-lost legacy IDs;
* derives authoritative `nortaxa/nortaxa_taxon_id` only from the compiler-proven `norwegian_taxon_id` invariant;
* preserves distinct concepts with identical names;
* preserves `nb`, `nn` and `se` literally;
* exports resolved Red List rows without treating them as identity;
* reports dangling parents without rewriting them;
* validates every emitted child `taxon_id` by streaming the JSONL;
* produces per-file hashes and a length-delimited whole-export hash;
* validates existing manifests on idempotent reruns;
* writes atomically and removes staging/backup directories;
* leaves the complete generated export untracked.

### Accepted test baseline

```text
44 exporter tests
285 pre-existing focused desktop taxonomy tests
329 total passing
```

### Rule for future agents

Do not redesign W1 while implementing W2.

A W1 change requires:

1. a newly identified exporter defect;
2. a focused regression test;
3. regenerated exact counts and hashes;
4. an explicit W1 acceptance review.

---

## 9. W1 repository integration and operational closeout

**Status: CLOSEOUT**

This is repository housekeeping, not a new engineering stage.

### Required actions

1. Open a PR from:

   ```text
   feat/taxonomy-v2-w1-cloud-exporter
   ```

   into:

   ```text
   integrate/taxonomy-media-2026-07-30
   ```

2. Confirm that the PR contains only source, tests and documentation.

3. Confirm that the generated export remains ignored and untracked.

4. Preserve the accepted W1 commit chain in the PR history or merge record.

5. Re-run the focused desktop taxonomy suite after resolving any conflicts.

6. Record the final merged commit in this document.

### Acceptance statement

```text
W1 implementation merged into the desktop taxonomy integration line.
```

W2A may proceed while the PR is under review because the W1 consumer contract is pinned to `04602bc`.

---

## 10. W2A — Additive Supabase schema, activation and search

**Status: DONE**

Implementation record:

```text
Web branch: feat/taxonomy-v2-w2-supabase-schema-search
Starting web commit: e964b36
Final web commit: d61f2f9
Migration: supabase/migrations/20260724130000_add_taxonomy_v2_schema_and_search.sql
```

### Verified implementation record

Model B was implemented with these exact tables:

```text
taxonomy_v2_releases
taxonomy_v2_concepts
taxonomy_v2_taxa
taxonomy_v2_scientific_names
taxonomy_v2_vernacular_names
taxonomy_v2_external_ids
taxonomy_v2_legacy_external_ids
taxonomy_v2_redlist
taxonomy_v2_import_runs
```

Exact RPC signatures:

```text
taxonomy_v2_validate_release(text) -> jsonb
taxonomy_v2_activate_release(text) -> jsonb
search_taxa_v2(text, text, integer) -> table
resolve_taxon_external_id_v2(text, text, text) -> table
```

Verified behavior:

* release validation reports structured expected/actual counts and errors;
* activation is advisory-locked, row-locked, validation-gated and transactional;
* failed activation leaves the prior active release unchanged;
* search implements the accepted exact/prefix rank order without collapsing
  distinct Sporely IDs;
* `no` is a query umbrella for literal `nb`, `nn`, and `no`, while other stored
  language codes remain distinct;
* resolver and search use only authoritative namespaced IDs and never consult
  namespace-lost legacy rows;
* all nine tables have RLS and explicit client revokes; read RPCs are granted to
  normal clients, while validation/activation remain service-role-only;
* the generated schema snapshot includes tables, constraints, indexes, comments,
  functions, owners, RLS, revokes and grants;
* no legacy taxonomy object and no client source file changed.

Verification evidence (2026-08-01):

```text
npx supabase db reset                                      exit 0
taxonomy_v2_schema_test.sql                               exit 0
taxonomy_v2_activation_test.sql                           exit 0
taxonomy_v2_search_test.sql                               exit 0
taxonomy_v2_security_test.sql                             exit 0
all other Supabase SQL tests                              exit 0, except known baseline below
public_observation_point_prep_test.sql                     exit 3 (known unrelated Not_set behavior)
npm test                                                  exit 1 (382 pass, known unrelated Leaflet CSS loader failure)
npm run build                                             exit 0
npx supabase db dump --local --schema public --file ...   exit 0
git diff --check                                          exit 0
```

Documentation was maintained from preflight through implementation rather than
reconstructed only at closeout. The web implementation contract is
`docs/taxonomy-v2-cloud-contract.md`; `SUPABASE_DB.md` contains the migration-
backed inventory.

W2A unresolved production blockers are intentionally deferred: the complete W1
release has not been imported, the W2B capacity gate has not run, and the
publication/provenance gate remains blocked. No production release was activated.

**W2A verdict:** W2A accepted — proceed to W2B importer and full-load validation.

### Purpose

Introduce cloud taxonomy-v2 structures and behavior using small SQL fixtures. Do not import the complete production release in W2A.

### Required architecture

Implement Model B using new `taxonomy_v2_*` objects:

```text
taxonomy_v2_releases
taxonomy_v2_concepts
taxonomy_v2_taxa
taxonomy_v2_scientific_names
taxonomy_v2_vernacular_names
taxonomy_v2_external_ids
taxonomy_v2_legacy_external_ids
taxonomy_v2_redlist
taxonomy_v2_import_runs
```

Stable identity belongs in `taxonomy_v2_concepts`.

Release-specific names, hierarchy, aliases, vernaculars, source bindings and assessments belong in release-scoped tables.

### Parent behavior

`taxonomy_v2_taxa.parent_sporely_taxon_id` must not have a strict self-referential FK in W2A.

Add a database comment explaining the accepted dangling-parent case.

### Required RPCs

```text
taxonomy_v2_validate_release(p_release_id text)

taxonomy_v2_activate_release(p_release_id text)

search_taxa_v2(
    q text,
    lang text default 'no',
    lim integer default 20
)

resolve_taxon_external_id_v2(
    p_source_system text,
    p_namespace text,
    p_external_id text
)
```

### Search rules

* Search the active release only.
* Return zero rows before the first activation.
* W2A search is prefix-based.
* Search canonical scientific names, scientific aliases and vernacular names in the selected language set.
* `no` expands to `nb`, `nn` and literal `no`.
* Retain one best match per Sporely ID.
* Never deduplicate across different Sporely IDs.
* Genus concepts are valid results.
* Ordering must be deterministic.
* COL preference is only an ordering tie-break.
* The namespace-lost legacy ID table is never consulted.

### Security

* Enable RLS on every new table.
* Give `anon` and `authenticated` no direct table reads.
* Expose only narrow `SECURITY DEFINER` RPCs.
* Use a controlled `search_path`.
* Grant activation only to service role/database owner.
* Revoke default function execution from `PUBLIC` before granting intended roles.

### Compatibility boundary

W2A must not change:

```text
public.taxa
public.taxa_vernacular
public.search_taxa(text, text, integer)
src/artsorakel.js
```

### Required fixture cases

* Two distinct `Cantharellus cibarius` concepts.
* Two distinct `Inocybe` genus concepts.
* `Candolleomyces candolleanus` with alias `Psathyrella candolleana`.
* Separate `Aureonarius limonius` and `Cortinarius limonius` concepts.
* Distinct `nb`, `nn` and `se` vernaculars.
* Authoritative COL and NorTaxa external IDs.
* A misleading legacy integer ID that resolvers must ignore.

### Required SQL tests

At minimum:

```text
taxonomy_v2_schema_test.sql
taxonomy_v2_activation_test.sql
taxonomy_v2_search_test.sql
taxonomy_v2_security_test.sql
```

### W2A acceptance gate

* Clean local Supabase reset succeeds.
* All new taxonomy tests pass.
* Existing Supabase tests are run.
* The known unrelated point-preparation failure is not hidden or reclassified.
* The schema snapshot is regenerated using the repository workflow.
* `npm test` runs.
* `npm run build` runs.
* No app source changes.
* No full W1 import.

Required verdict:

```text
W2A accepted — proceed to W2B importer and full-load validation
```

---

## 11. W2B — Importer, full local load and capacity validation

**Status: DONE — IMPORTER EXPERIMENT VALID; PRODUCTION SCOPE REJECTED**

Implementation and evidence record (2026-08-01):

```text
Web branch: feat/taxonomy-v2-w2b-importer-full-load
Starting web commit: d61f2f9692b3b9e803d265330e21ddf876b26802
Final web commit: e05979b
Evidence: docs/evidence/taxonomy-v2/w2b-tax-2026.07.30-02.{json,md}
```

Verified results:

* the streaming Node/container-psql importer preflighted and loaded the complete
  accepted W1 artifact in one locked transaction;
* all exact release-table counts matched and
  `taxonomy_v2_validate_release` returned `ok=true`;
* the importer left the release ready; a separate local-only activation was used
  for correctness and performance evidence;
* taxonomy-v2 relations occupy 754,417,664 bytes (719.469 MiB);
* the read-only production baseline is 103,304,339 bytes (98.519 MiB);
* the additive projection is 857,722,003 bytes (817.987 MiB), with
  -333,434,003 bytes headroom below 500 MiB;
* capacity result is `review_required_capacity`; no legacy size was subtracted;
* selective search p50 is approximately 0.65–0.71 seconds and requires a
  representation/index review before cutover; authoritative resolver p50 is
  0.737 ms;
* no production taxonomy import or activation, client change, legacy taxonomy
  mutation, observation schema change, or W3 work occurred.

W2B is accepted as evidence that the representation is technically importable.
It is rejected as a production runtime scope and architecture. The next safe
task is W2C macrofungi-scope verification and sparse-registry design. W3 remains
blocked. The publication/provenance gate remains independently blocked.

## 11A. W2C — Global macrofungi scope and sparse-registry design

**Status: NEXT — W3 REMAINS BLOCKED**

The earlier compact-schema experiment remains useful size and search evidence,
but its 634,894-concept full-Fungi candidate is not the selected production
architecture. W2C is redefined to produce two reviewed outputs:

1. a COL-ID-keyed global macrofungi policy and measured desktop search pack;
2. a sparse Supabase registry contract for only materialized Sporely concepts.

The current executable `database/taxonomy/policies/scope.yml` and
`database/taxonomy/desktop-compatibility.json` continue to describe or pin the
historical broad candidate. This documentation-only correction does not change
them. They must be revised and validated in the future implementation stage
before any new macrofungi artifact is built or published.

### Product definition

> A macrofungus is a fungus that normally produces a macroscopic,
> field-observable fruiting or reproductive structure that a person could
> deliberately photograph, inspect or collect as an organismal observation.

This is product scope, not a formal rank. It generally includes agarics,
boletes, chanterelles, polypores, observable corticioid fungi, tooth fungi,
corals and clubs, puffballs and related gasteroid fungi, jelly fungi, morels,
false morels, cup fungi, earth tongues, observable stromatic ascomycetes, and
hypogeous macrofungi/truffles. It generally excludes yeasts, moulds, powdery
mildews, ordinary microscopic leaf-spot fungi, environmental or sequence-only
concepts, lichenized fungi unless separately authorized, fungus-like organisms
outside Fungi, and rusts/smuts except reviewed field-recordable exceptions.
All Basidiomycota are not automatically macrofungi, and all Ascomycota are not
automatically microfungi.

Plants are not part of taxonomy-v2 or its macrofungi pack. Habitat trees and
plants may later use controlled habitat or associated-tree categories, free
text, or external organism discovery; no global or Norwegian plant taxonomy is
to be imported into Supabase.

### Provisional clade policy

Implementation must verify these human-readable seeds against the pinned COL
release and persist stable COL concept identifiers rather than name strings:

* broadly include descendants of `Agaricomycetes`, `Dacrymycetes`,
  `Pezizomycetes`, `Geoglossomycetes`, and `Neolectomycetes`;
* include only reviewed fruit-body-forming Tremellomycetes lineages; `Tremella`,
  `Phaeotremella`, `Naematelia`, and `Sirobasidium` are non-exhaustive seeds;
* exclude `Pucciniomycotina` and `Ustilaginomycotina` by default, but include
  genus `Gymnosporangium` and species `Ustilago maydis` explicitly;
* classify `Atractiellomycetes` as review-required/optional and do not preload
  it initially without evidence;
* include reviewed Leotiomycetes families/genera, seeded by `Leotia`,
  `Microglossum`, `Cudonia`, `Spathularia`, `Chlorociboria`, `Ascocoryne`,
  `Bulgaria`, and `Cyttaria`, using current COL placement;
* include reviewed visibly stromatic Sordariomycetes lineages, including
  `Xylariaceae`, `Hypoxylaceae`, selected `Diatrypaceae`, and reviewed visibly
  fruiting Hypocreales such as `Cordyceps`, `Ophiocordyceps`, selected
  `Tolypocladium`, `Podostroma`, and stromatic `Trichoderma` lineages;
* exclude Eurotiomycetes/Eurotiales broadly; review `Elaphomyces` and measure
  whether all `Elaphomycetaceae` is suitable;
* exclude remaining Ascomycota by default unless an explicit reviewed rule
  includes it. `Tuber` needs no special rule because Pezizales already covers it.

Policy precedence is explicit species, genus, family/higher clade, broadly
included class, broadly excluded subphylum/class, then default exclude. Lower
ranks override broader rules. Every eventual rule records stable COL concept
ID, display name, rank, include/exclude/review state, reason, evidence, source
release, exceptions, and review date.

### Source roles and sparse cloud model

COL XR remains the global identity backbone. NorTaxa supplies Norwegian
mappings/names, Artsorakel reconciliation, and legacy continuity; national Red
Lists supply regional conservation enrichment only. iNaturalist may provide
international discovery/text search and Artsorakel image identification. Every
external ID stays source- and namespace-specific, with selected values retained
as provenance. None of these regional or discovery sources replaces COL or
defines the global preload.

Supabase materializes a concept only when used by an existing observation,
selected through an external service, received from desktop sync, accepted by a
manual-resolution workflow, explicitly curated, or seeded into an approved
small search cache. It retains the Sporely ID, canonical identity snapshot,
namespaced mappings, selected-name snapshots, source/release provenance, scope
reason, and review state. Scientific-name equality is never identity.

W3 observation identification must snapshot the resolved Sporely ID when
available, selected scientific and vernacular names, rank, source system and
namespace, raw external ID, release or response timestamp, selection timestamp,
resolution state, and original selected result. Historical display cannot
depend on a live API or surviving cache entry.

Concepts outside the current preload remain resolvable when referenced by an
observation, legacy mapping, import, or manual historical record. Discovery
inclusion, sparse-registry materialization, historical preservation, and current
product scope are separate states. Existing observations are never deleted or
rewritten merely because a concept is outside the preload.

W2C does not authorize W3, production taxonomy writes or activation, client
cutover, or Red List publication.

### Purpose

Build a repeatable importer for the accepted W1 artifact, load it into local Supabase/Postgres and measure the actual database cost.

### Import mechanism

Preferred direction:

```text
A versioned admin/CI script using a direct PostgreSQL connection
```

The importer must not depend on PostgREST streaming `COPY FROM STDIN`.

The agent must inspect repository deployment conventions before fixing the implementation language or location. Python PostgreSQL tooling or Node PostgreSQL tooling are both acceptable when justified.

### Importer requirements

1. Read and validate `taxonomy_export_manifest.json`.
2. Verify all seven file hashes and `whole_export_sha256`.
3. Require exact release, schema and scope identifiers.
4. Create a `taxonomy_v2_import_runs` audit row.
5. Load into staging or into an unactivated release with status `loading`.
6. Use bounded streaming or bulk copy.
7. Load tables in dependency order.
8. Preserve null versus empty string.
9. Map W1 fields without semantic invention.
10. Keep legacy integer IDs in the audit-only table.
11. Validate exact row counts and namespace counts.
12. Run `taxonomy_v2_validate_release`.
13. Mark the release `ready`, never automatically `active`.
14. On failure, mark the run/release failed and leave the current active release untouched.
15. Be idempotent for an already loaded identical release.
16. Reject a release ID whose hashes differ from an existing database record.

### Full-load measurements

Measure for every new table and index:

```text
pg_relation_size
pg_indexes_size
pg_total_relation_size
```

Also record:

* total database size before import;
* total database size after import;
* temporary peak size during import and index creation;
* import runtime;
* index creation runtime;
* validation runtime;
* search p50 and p95 over a fixed query corpus.

### Capacity gate

The accepted JSONL is approximately 527 MB uncompressed. PostgreSQL storage cannot be estimated reliably from JSONL size alone.

Before production import, calculate:

```text
projected production total
= current production database bytes
+ measured taxonomy-v2 table/index bytes
```

Because the rollout is additive, do not subtract legacy taxonomy size before it has actually been removed.

Provisional stop thresholds:

```text
projected total >= 350 MiB

or

remaining headroom below a 500 MiB limit < 150 MiB
```

Crossing either threshold requires an explicit hosting or capacity decision.

Do not silently reduce the data by dropping identity or provenance fields.

### Permitted optimization discussion after measurement

* Exclude legacy namespace-lost IDs from the production database while retaining them in the complete W1 artifact.
* Retain only active and immediately previous release snapshots.
* Defer non-search indexes.
* Move archival releases outside the primary application database.
* Upgrade database capacity.
* Define a separate compact search projection derived from the complete release.

Any reduced projection requires a new explicit contract and equivalence tests. W1 remains the complete source artifact.

### W2B acceptance gate

```text
Importer repeatable
Full local load validated
Exact table and index sizes recorded
Search performance measured
Capacity decision documented
Release status ready but not active
```

---

## 12. Publication and provenance gate

**Status: BLOCKED for production activation**

The current taxonomy release is a candidate, not a published release.

Before production activation:

1. Verify and archive the applicable Artsdatabanken and Red List data terms.
2. Record the licence identifier, source URL, retrieval time and evidence hash.
3. Resolve null or incomplete release-provenance fields.
4. Update the release-building source of truth, not only the exported W1 JSON.
5. Rebuild the compiled taxonomy artifact when source-manifest bytes change.
6. Regenerate the W1 export.
7. Accept the new W1 hashes.
8. Mark the release publication state appropriately.
9. Make W2 validation reject production activation of a non-publishable release.

Local development and performance work may use the candidate release.

Production activation may not.

---

## 13. W3A — Observation identity schema

**Status: PLANNED**

### Purpose

Add stable taxonomy identity to observations without removing existing genus, species or manual-text fields.

### Required observation semantics

At minimum, the cloud model must represent:

```text
sporely_taxon_id          nullable stable concept ID
identity_state            resolved | unresolved | provisional | manual
scientific_name_snapshot  selected/displayed name at decision time
taxon_rank_snapshot       selected rank at decision time
taxonomy_release_id       release used for the decision, when applicable
```

The W3 agent must inspect the actual observation schema and choose final names that do not collide with existing fields.

### Evidence model

Use an `observation_identifications` table or equivalent evidence structure when needed to retain:

* source service or user action;
* source system;
* namespace and external ID;
* scientific-name and rank snapshots;
* probability or confidence;
* raw or normalized service evidence;
* identity state;
* timestamps;
* supersession or selected-result relationship.

Do not force all historical identification evidence into the main observation row.

### Required behaviors

| Action                                                                | Expected identity result                          |
| --------------------------------------------------------------------- | ------------------------------------------------- |
| User selects an exact species concept                                 | Non-null Sporely ID, `resolved`, species snapshot |
| User selects a genus concept                                          | Non-null Sporely ID, `resolved`, rank `genus`     |
| AI/service proposes an exact namespaced candidate before confirmation | Evidence retained; normally `provisional`         |
| User enters unmatched text                                            | Null Sporely ID, `manual`                         |
| Result is ambiguous and the user has not chosen                       | Null Sporely ID, `unresolved`, evidence retained  |
| User explicitly clears identification                                 | Null Sporely ID and cleared identity fields       |
| Sync payload omits identity keys                                      | Preserve existing cloud identity                  |

### Foreign-key policy

A nullable FK from observations to `taxonomy_v2_concepts` is acceptable after the stable concept table is deployed.

Do not reference release-scoped taxonomy rows from observations.

### W3A acceptance gate

* Migration is additive.
* Existing observations remain valid.
* State constraints are exact.
* Existing RLS and triggers remain effective.
* Old clients can write observations without v2 fields.
* Tests pin missing-key versus explicit-null behavior.

---

## 14. W3B — Desktop/web sync and authoritative backfill

**Status: PLANNED**

### Sync path

Update payloads so taxonomy identity survives:

```text
desktop database or web state
→ review/capture state
→ upload payload
→ Supabase observation
→ pull payload
→ desktop import/update
```

The current desktop firewall around v2 observation fields must be removed only after W3A is deployed.

### Backfill hierarchy

Backfill only from evidence that meets the accepted identity contract.

Preferred order:

1. Existing trusted Sporely ID, when valid.
2. Exact authoritative `(source_system, namespace, external_id)` match.
3. Existing legacy taxonomy ID only when a documented deterministic bridge exists.
4. Scientific-name candidates for reporting or manual review only.

Name-only matching must not silently resolve identity.

### Backfill report

The dry-run must report:

```text
already resolved
resolved uniquely
ambiguous
no candidate
invalid source ID
manual/free text
conflicting evidence
```

Ambiguous and unmatched observations remain unresolved.

### Required sync tests

* Desktop changed identity, cloud unchanged.
* Cloud changed identity, desktop unchanged.
* Both changed.
* Missing identity keys.
* Explicit null identity.
* Genus-only identity.
* Manual text.
* Old client payload.
* Provisional AI evidence.

### W3B acceptance gate

* Dry-run counts reviewed.
* No name-only silent resolution.
* Push/pull round trips preserve state and snapshots.
* Legacy clients remain compatible.
* Backfill is restartable and auditable.

---

## 15. W4A — Web taxonomy service and picker

**Status: PLANNED**

### Purpose

Introduce taxonomy-v2 search to the web app without immediately deleting the legacy path.

### Service structure

Create a dedicated taxonomy-v2 service module rather than placing more taxonomy responsibilities inside AI request code.

The normalized application result should include at least:

```text
sporelyTaxonId
parentSporelyTaxonId
taxonRank
genus
specificEpithet
scientificName
family
vernacularName
vernacularLanguage
canonicalSourceSystem
canonicalExternalId
colUsageId
nortaxaTaxonId
matchedName
matchedLanguage
matchType
```

### Picker behavior

* Preserve distinct same-name concepts.
* Show rank and source context where required to distinguish results.
* Support genus-only selection.
* Preserve manual entry.
* Never infer identity because a displayed string happens to match.
* Store the selected Sporely ID and name/rank snapshots.
* Do not collapse `nb` and `nn` in persistence.
* Allow `no` as a search preference umbrella.

### Rollout modes

Use a feature flag or controlled mode:

```text
legacy
shadow
v2
```

In shadow mode, users continue to see legacy behavior while v2 results are compared without changing persisted identity.

### W4A acceptance gate

* Unit tests for RPC result mapping.
* UI tests for ambiguity.
* UI tests for genus-only.
* Manual entry remains available.
* Legacy fallback remains available.
* No production default switch before W5.

---

## 16. W4B — Artsorakel resolution, evidence and ambiguity UX

**Status: PLANNED**

### Identity resolution rule

Artsorakel or another service may bind to a Sporely concept only through an exact authoritative namespace-aware external ID.

Use:

```text
resolve_taxon_external_id_v2(
    source_system,
    namespace,
    external_id
)
```

Never use a scientific-name fallback to establish identity.

### Service-result handling

* Retain service name and version.
* Retain raw service taxon ID and namespace.
* Retain probability or confidence.
* Retain service-provided name and rank snapshots.
* Resolve all exact candidates returned by the authoritative resolver.
* If there are zero candidates, retain unresolved evidence.
* If there are multiple candidates, show ambiguity.
* A service suggestion normally remains `provisional` until user confirmation.

### User-facing states

The UI must distinguish:

```text
Confirmed identification
Suggested identification
Manual text
Unresolved service result
Ambiguous mapping
```

Where two concepts share a name, display enough context for a safe choice:

* rank;
* canonical source;
* family when available;
* accepted-name versus alias match;
* relevant national ID.

Do not expose unnecessary provenance detail in ordinary unambiguous cases.

### W4B acceptance gate

* Unique exact external-ID path.
* Multiple-match path.
* No-match path.
* Alias display.
* User-confirmed provisional-to-resolved transition.
* Service result never overwrites a later manual selection.

---

## 17. W5 — Validation, activation, cutover and rollback

**Status: PLANNED**

### Pre-cutover comparison

Run a fixed corpus through legacy and v2 search:

* canonical species exact;
* aliases;
* vernacular exact and prefix;
* genus-only;
* same-name distinct concepts;
* Bokmål/Nynorsk umbrella behavior;
* Sámi language;
* known Artsorakel IDs;
* known legacy mismatch cases.

Record:

```text
result overlap
intentional differences
unexpected losses
unexpected merges
ordering differences
p50 latency
p95 latency
```

### Release activation

1. Validate the target release.
2. Confirm the publication/provenance gate.
3. Confirm the capacity gate.
4. Acquire the activation lock.
5. Retire the old active release.
6. Activate the target transactionally.
7. Verify RPC health immediately.

Activating a release does not itself switch clients from legacy search to v2 search.

### Client rollout

Recommended phases:

```text
1. Internal/dev v2
2. Shadow comparison
3. Small controlled cohort
4. All new sessions
5. Remove legacy fallback after the stability window
```

### Monitoring

Track:

* search errors;
* zero-result rate;
* ambiguity rate;
* RPC latency;
* resolver no-match and multiple-match rates;
* identity-state distribution;
* observation save failures;
* sync conflicts;
* database growth.

### Rollback

Rollback must remain possible by:

* switching the client back to legacy mode;
* reactivating the previous v2 release when appropriate;
* preserving observation snapshots;
* retaining legacy taxonomy throughout the initial cutover.

### W5 acceptance gate

* Production release is active.
* Web v2 is stable as the default.
* Desktop/web identity round trips are stable.
* Rollback has been tested.
* Monitoring shows no unacceptable regressions.

---

## 18. Legacy retirement and release retention

**Status: PLANNED**

Do not retire legacy taxonomy merely because v2 works locally.

### Retirement prerequisites

* W5 stability window completed.
* No supported client still calls legacy `search_taxa`.
* Rollback window expired.
* Production backups verified.
* Database size measured before and after.
* Old observations remain readable.
* Old identifiers have a documented bridge or snapshot behavior.

### Retirement sequence

1. Remove the client fallback.
2. Revoke unused legacy RPC execution.
3. Archive legacy row counts and schema information.
4. Drop legacy indexes/tables in a separate migration.
5. Re-measure database size.
6. Update `SUPABASE_DB.md`.
7. Retain rollback instructions or a restorable dump.

### Release retention

The initial policy should preserve:

```text
active release
immediately previous release
```

Additional retired releases depend on measured storage.

Stable concepts remain. Release-scoped snapshots may be archived only after an explicit retention decision.

---

## 19. Test, performance and evidence matrix

| Area               | Required evidence                                                                   |
| ------------------ | ----------------------------------------------------------------------------------- |
| Desktop W1         | Exporter tests, focused taxonomy tests, exact hashes/counts and deterministic rerun |
| Supabase schema    | Clean reset, constraints, FKs, RLS, grants and legacy compatibility                 |
| Activation         | Valid activation, failed activation rollback and one-active invariant               |
| Search             | Canonical, alias, vernacular, rank, language, ambiguity and deterministic order     |
| Resolver           | Exact namespace match, zero/multiple matches and exclusion of the legacy table      |
| Importer           | Manifest/hash verification, exact counts and restart/failure behavior               |
| Capacity           | Per-table/index sizes, database total before/after and projected production total   |
| Performance        | Fixed corpus, p50/p95, query plans and index use                                    |
| Observation schema | Old-client compatibility, state constraints and nullable stable FK                  |
| Sync               | Push/pull, missing/null, conflicts, genus/manual/provisional                        |
| Backfill           | Dry-run categories, no silent ambiguity and restartability                          |
| Web                | Mapping tests, picker ambiguity, manual entry and genus-only                        |
| Artsorakel         | Exact external ID, no match, multiple matches and provisional confirmation          |
| Cutover            | Shadow comparison, rollout flag, rollback test and monitoring                       |

Known unrelated baselines must remain visible:

```text
Web Leaflet CSS environment failure, when still present

Supabase public_observation_point_prep_test.sql
Not_set behavior
```

Agents must not modify unrelated behavior inside taxonomy stages unless separately authorized.

---

## 20. Agent operating protocol, handoff template and master checklist

### Start-of-task protocol

Every agent begins with:

```bash
pwd
git status
git branch --show-current
git log --oneline --decorate --max-count=10
git diff --stat
```

The agent then:

1. Confirms the repository and branch.
2. Reads this plan and the current-stage contract.
3. Inspects the relevant implementation and latest migrations.
4. States the stage boundary.
5. Reports contradictions between the plan and repository.
6. Implements only the current stage.
7. Runs required tests.
8. Commits intentionally.
9. Pushes the branch.
10. Returns evidence.

### Mandatory handoff report

```text
Stage:
Repository:
Branch:
Starting commit:
Final commit:
Files changed:
Architecture implemented:
Migrations and RPCs:
Data counts and hashes:
Commands run with exit codes:
Tests passed:
Known unrelated failures:
Generated or untracked artifacts:
Security and RLS findings:
Capacity and performance findings:
Stop conditions encountered:
Items explicitly deferred:
Next stage:
Verdict:
```

### Stop conditions

An agent must stop rather than guess when:

* an external-ID namespace is unclear;
* distinct concepts would be merged;
* name-only matching would establish identity;
* language codes would be rewritten;
* a migration would alter legacy taxonomy before W5;
* production activation would use a candidate or unpublished release;
* full import would exceed the capacity gate;
* normal clients would need direct taxonomy-table access;
* active-release switching is not transactionally safe;
* observed repository behavior contradicts an accepted decision;
* completing the current stage would require implementing the next stage.

### Decision log

| Decision                                               | State                    |
| ------------------------------------------------------ | ------------------------ |
| Stable Sporely concept ID                              | Accepted                 |
| COL canonical authority                                | Accepted                 |
| NorTaxa national enrichment and authoritative taxon ID | Accepted                 |
| W1 model-neutral output                                | Accepted                 |
| Model B cloud decomposition                            | W2A baseline only        |
| Compact purpose-built runtime candidate                | Accepted for W2D         |
| Seven-file W1 export                                   | Accepted                 |
| Authoritative/legacy external-ID split                 | Accepted                 |
| `no` as query umbrella only                            | Accepted                 |
| Red List not identity                                  | Accepted                 |
| Preserve dangling parent and omit W2A self-FK          | Accepted                 |
| Additive rollout through W5                            | Accepted                 |
| Split W2 into W2A and W2B                              | Accepted                 |
| Do not commit the complete W1 export                   | Accepted                 |
| Production publication/licence gate                    | Open blocker             |
| Production cloud architecture                          | External discovery plus sparse registry |
| Retired-release retention beyond previous release      | Artifact/metadata only; one rollback slot |
| Final cutover stability-window duration                | Pending W5 rollout plan  |

### Master completion checklist

```text
[x] W0 audit accepted
[x] W0 input/output contracts closed
[x] W1 exporter implemented
[x] W1 validation defects corrected
[x] W1 identity contract finalized
[x] W1 accepted at 04602bc

[ ] W1 merged into desktop integration branch

[x] W2A schema migration implemented
[x] W2A RPCs implemented
[x] W2A SQL and security tests accepted

[x] W2B importer implemented
[x] Complete local release imported
[x] PostgreSQL storage measured
[x] Search performance measured
[x] Full-Fungi W2B importer experiment measured
[x] Full-Fungi production runtime scope rejected
[ ] W2C pinned-COL macrofungi rules reviewed and measured
[ ] W2C sparse-registry contract accepted
[ ] W2D production migration and materialization workflow implemented
[ ] W2D partition publication/reclamation test accepted

[ ] Publication and licence evidence closed
[ ] Release marked publishable

[ ] W3A observation schema deployed
[ ] W3B sync implemented
[ ] Authoritative backfill dry-run reviewed

[ ] W4A v2 picker implemented
[ ] W4B Artsorakel resolver implemented

[ ] Shadow comparison accepted
[ ] Production release activated
[ ] Client v2 rollout completed
[ ] Rollback test completed

[ ] Legacy taxonomy retired
[ ] Final documentation and runbook completed
```

The next engineering task is **W2D compact schema implementation only**.

Do not begin W3 or W4 until W2D is separately accepted. Do not publish Red List
enrichment until provenance/licensing is resolved.
