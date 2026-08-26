# W3-A local rehearsal — evidence

Local production-integration design proved against a disposable
Supabase stack. **No** production access, migration creation, migration
application, or client cutover.

## Frozen authoritative artefacts

Full manifest: [`w3a-authoritative-artifacts.json`](w3a-authoritative-artifacts.json).

Current chain:

| role | release_id | shard SHA-256 |
|---|---|---|
| base | `tax-2026.08.01-01` | scope-manifest `72758b2c574e8aea27432b6b55c62dfb6ad87f3fadc11ad1c892a61abf23ac4e`; export-manifest `52620f72e428709bbf1f7b01d3999386ab8c08039f4a56c6fa7cc9e7631d7b8f` |
| supplement | `tax-2026.08.02-02` | `6c95612b83fbf684d9db7c66fe515b2225e57c8b3b6ceb03e001867fad41067b` |
| supplement | `tax-2026.08.03-02` | `dedf0fbc1d691b8f716e8ece3a365d1ab3835474912bef5fc9a527fec784eadd` (depends_on `tax-2026.08.02-02`) |

Superseded and retained as **unpublished build candidates** (bytes not
in Git; only the SHAs live in prior committed evidence):

| release_id | shard SHA-256 | why superseded |
|---|---|---|
| `tax-2026.08.02-01` | `a7cf966b71530b56e7b9cd544d45c2959c28752b13c22d623c34fbf7f8cb158a` | shard emitted alias before its accepted-target anchor; `IdentityRegistry.load()` refuses that ordering |
| `tax-2026.08.03-01` | `bdfdb8c08bf48e9c457d31c009f6ee9446d6f485e010a35d90f221f3a281e947` | contained 19 iNaturalist-native canonical anchors — rejected by the accepted architecture |

## Deliverables

* Schema draft: [`sporely-web/supabase/drafts/taxonomy_v3_schema.sql`](../../../../../sporely-web/supabase/drafts/taxonomy_v3_schema.sql)
  — isolated schema `taxonomy_v3`, RLS on every table, immutable-snapshot trigger, hardened `external_mapping` conflict invariant, `install_release_chain()` transactional installer.
* Observations integration draft: `sporely-web/supabase/drafts/taxonomy_v3_observations_integration_draft.sql`
  — additive nullable column, service-role write guard, `link_observations_to_resolution()`.
* Local installer: `sporely-web/scripts/taxonomy-v3/install-release-chain.mjs`
  — refuses `--production`; validates chain hashes on load; single-transaction apply; idempotent; rejects release-ID reuse with different bytes; rejects external-mapping conflicts.
* Rehearsal harness: `sporely-web/scripts/taxonomy-v3/w3a-rehearsal.test.mjs`.
* Compatibility audit: [`w3a-compatibility-audit.md`](../../docs/w3a-compatibility-audit.md).
* Rollback plan (two modes): [`w3a-rollback-plan.md`](../../docs/w3a-rollback-plan.md).
* Operator runbook: [`w3a-operator-runbook.md`](../../docs/w3a-operator-runbook.md).

## Proposed schema (short form)

| table | purpose | write access |
|---|---|---|
| `taxonomy_v3.registry_concept` | sparse canonical registry (39 in-cache + 103 out-of-cache after apply); cache_state and scope_state stored **independently** | service_role only |
| `taxonomy_v3.external_mapping` | `(source_system, namespace, external_id)` UNIQUE, hardened conflict invariant | service_role only |
| `taxonomy_v3.identification_snapshot` | immutable historical snapshot (trigger blocks `UPDATE original_*` while `snapshot_locked=true`) | service_role only |
| `taxonomy_v3.resolution_link` | mutable canonical resolution pointer per observation | service_role only |
| `taxonomy_v3.release_installation` | audit — every base/supplement install; release-ID reuse with different hashes raises | service_role only |
| `taxonomy_v3.supplement_installation` | audit — depends_on graph enforced by FK | service_role only |
| `taxonomy_v3.reconciliation_manifest_audit` | audit — one row per manifest semantic SHA | service_role only |

Public reads: `registry_concept`, `external_mapping`, `resolution_link`
only. Everything else default-denies for anon+authenticated.

Observations integration (additive):

* `public.observations.resolved_sporely_taxon_id integer` — nullable FK
  to `taxonomy_v3.registry_concept`; `ON DELETE SET NULL`.
* Write guard trigger — only `service_role` (or `postgres` superuser)
  may change the column.
* `taxonomy_v3.link_observations_to_resolution()` — one UPDATE that
  copies canonical links from `resolution_link` to
  `public.observations.resolved_sporely_taxon_id`; touches no other
  observation field.

## Migration phases

1. Install schema draft on local disposable stack (**W3-A, done**).
2. Human-authorised port to `supabase/migrations/` (**NOT W3-A**).
3. Install release chain on staging via `install_release_chain()`.
4. Apply reconciliation manifest on staging.
5. Run `link_observations_to_resolution()`; smoke-test read paths.
6. Dual-read client rollout with feature flag off by default.
7. Feature-flag flip to prefer canonical link where non-null.
8. Dual-write RPC for AI-identification.
9. Legacy-column deprecation (much later).

## Release installation counts (local)

| table | count |
|---|---:|
| `release_installation` | 3 (1 release + 2 supplements) |
| `supplement_installation` | 1 (only `tax-2026.08.03-02` declares a depends_on) |
| `reconciliation_manifest_audit` | 1 |

## Historical reconciliation counts (local)

| dimension | count |
|---|---:|
| `identification_snapshot` | **369** (immutable) |
| `resolution_link` — total | 369 |
| `resolution_link` — resolved (non-null canonical) | **233** |
| `resolution_link` — NULL canonical | **136** (21 unresolved_external + 85 manual_unresolved + 30 no_identity_evidence) |
| `registry_concept` — in_cache | 39 |
| `registry_concept` — out_of_cache | 103 |
| `external_mapping` | 144 |
| second-apply added rows | **0** (fully idempotent) |

## Invariants proven (rehearsal)

| invariant | verdict |
|---|---|
| 369 identification snapshots created | ✓ |
| 233 canonical links attached | ✓ |
| 136 observations retained with null canonical links (21+85+30) | ✓ |
| original observation fields unchanged | ✓ (integration draft only writes `resolved_sporely_taxon_id`; trigger blocks UPDATE of `identification_snapshot.original_*`) |
| second apply produces no duplicate rows | ✓ (row counts identical after twice apply) |
| later exact resolution updates only the mutable link | ✓ (`row_to_json(identification_snapshot)` before vs after equal) |
| release-ID reuse with different hashes fails closed | ✓ |
| external-mapping conflicts cause full rollback | ✓ (zero orphan rows post-failure) |
| search-cache membership unchanged | ✓ (base `taxon_external_id.jsonl` remains 52,881 rows; supplements add only out-of-cache concepts) |
| RLS blocks client writes to canonical mappings | ✓ (client-facing tables grant SELECT only; `identification_snapshot` default-denied) |

Full test output: **9 tests, 9 pass, 0 skipped** for
`W3A_INTEGRATION=1 node --test scripts/taxonomy-v3/w3a-rehearsal.test.mjs`.

## Rollback modes

Two modes designed. Mode 1 (before cutover) proven locally by every
rollback test in the rehearsal. Mode 2 (after cutover) documented but
not exercised — there is no cutover to roll back yet. See
[`w3a-rollback-plan.md`](../../docs/w3a-rollback-plan.md).

## Client compatibility findings

* 14 read/write paths audited across sporely-web (8) and sporely-py (6);
  **zero** break under W3-A.
* Deferred to later stages:
  * `sporely-web/src/screens/find_detail.js` (dual-read)
  * `sporely-web/src/ai-identification.js` (dual-write via RPC)
* No path requires a hard cutover during W3-A. Legacy taxonomy columns
  remain the authoritative reads.

Full audit: [`w3a-compatibility-audit.md`](../../docs/w3a-compatibility-audit.md).

## Remaining blockers

* Porting `supabase/drafts/*.sql` into `supabase/migrations/` requires
  human authorisation and its own review pass.
* Operator must rebuild the reconciliation manifest locally each time
  `/tmp/` is reaped; the manifest itself is **not** committed.
* A dual-read `search_taxa_v3` RPC must be designed for the cutover
  phase.

## Exact production steps requiring human authorization

1. `supabase link` + `supabase migration new` (creating a real migration
   from the draft SQL files).
2. `supabase migration up` on staging.
3. Manually running `install-release-chain.mjs` against a staging
   Supabase project.
4. Granting `service_role` to any operator or CI environment.
5. Toggling any client feature flag from legacy-only to dual-read.

## Safety statement

* production access: **no**
* production writes: **no**
* production migration creation: **no**
* production migration application: **no**
* client cutover: **no**
* desktop taxonomy activation: **no**
* legacy-field removal: **no**
* real manifest committed: **no**
