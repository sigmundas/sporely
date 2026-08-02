# W3-A operator runbook

**Purpose:** guide the authorised operator through a **local**
rehearsal of the production integration. Nothing in this document
authorises production access.

## Preconditions

1. Local Supabase stack up on OrbStack (`supabase status` reports
   running containers).
2. Reconciliation manifest at
   `/tmp/w2ebc-real-1/reconciliation-manifest.json` with semantic SHA-256
   `1beaa33f3891b216d3bc7c6d34cd96df1a936627c5a6f749a515cc75d51c094e`
   and 369 records. Rebuild via
   `python -m database.taxonomy.reconciliation.cli --input <snapshot> --output /tmp/w2ebc-real-1 --release-dir <base> --policy <policy> --canonical-registry <base-registry> --supplement <sup1> --supplement <sup2>`
   when the /tmp path has been reaped.
3. Supplement directories at `/tmp/w2ea2v2-supp-a/` and
   `/tmp/w2ebv2-supp-a/`. Rebuild via
   `python -m database.taxonomy.scripts.allocate_observation_derived_anchors …`
   and `allocate_recovered_external_anchors …` when needed.

## Rehearsal (local only)

Run from `sporely-web`:

```
W3A_INTEGRATION=1 node --test scripts/taxonomy-v3/w3a-rehearsal.test.mjs
```

Expected: **9/9 pass, 0 skipped.** The rehearsal:

1. Applies `supabase/drafts/taxonomy_v3_schema.sql` to the disposable
   local Supabase (never `supabase migration up`).
2. Applies `supabase/drafts/taxonomy_v3_observations_integration_draft.sql`
   (additive; `add column if not exists`, guarded).
3. Calls `taxonomy_v3.install_release_chain(base, supplements,
   manifest)` — one transaction, atomic.
4. Asserts the accepted counts (233 / 21 / 85 / 30) exactly.
5. Exercises identification-snapshot immutability, later-exact
   resolution, release-ID reuse rejection, external-mapping conflict
   rollback, and the observations-link function.

## Manual local install (mirrors the runbook step-by-step)

```
node scripts/taxonomy-v3/install-release-chain.mjs \
  --base ../sporely-py/database/reference_data/generated/taxonomy_v2/global_macrofungi_tax-2026.08.01-01 \
  --supplement /tmp/w2ea2v2-supp-a \
  --supplement /tmp/w2ebv2-supp-a \
  --manifest /tmp/w2ebc-real-1/reconciliation-manifest.json \
  --observations-integration
```

The installer:

* discovers the local Supabase container via
  `scripts/taxonomy-v2/lib/docker-psql.mjs discoverLocalTarget()` (fails
  closed if the container is not the expected local project);
* refuses `--production` (exit code 3);
* runs the whole chain under one call to
  `taxonomy_v3.install_release_chain()` — any raise rolls back
  everything the call added.

## Verification queries (run inside the disposable schema)

```
select artifact_kind, count(*) from taxonomy_v3.release_installation group by artifact_kind;
select cache_state, count(*)   from taxonomy_v3.registry_concept    group by cache_state;
select resolution_state, count(*) from taxonomy_v3.resolution_link  group by resolution_state;
select observation_id from taxonomy_v3.identification_snapshot limit 3;
```

Expected shape (per the rehearsal):

| release_installation | count |
|---|---:|
| release | 1 |
| registry_supplement | 2 |

| cache_state | count |
|---|---:|
| in_cache | 39 |
| out_of_cache | 103 |

| resolution_state | count |
|---|---:|
| resolved_exact | 233 |
| unresolved_external_identifier | 21 |
| manual_unresolved | 85 |
| no_identity_evidence | 30 |

## Rollback

See [`w3a-rollback-plan.md`](w3a-rollback-plan.md).

## What later stages will need (human authorisation)

These steps are **not** part of W3-A. Listing them so the operator can
plan.

1. Convert the two draft SQL files into a real Supabase migration under
   `supabase/migrations/YYYYMMDD_taxonomy_v3.sql`. Preserve the
   `taxonomy_v3` schema name and RLS policies verbatim. Review with
   supabase/README.md rules.
2. Provide the reconciliation manifest as a service-role-restricted
   artefact (never committed): the manifest itself contains anonymised
   observation IDs, but it is still customer-linkable data.
3. Add a taxonomy_v3-aware `search_taxa_v3` RPC that consults the
   sparse registry with fallback to the base release cache; grant
   `select` to `anon`+`authenticated`, `execute` on RPC to
   `authenticated`.
4. Add the dual-read path in `sporely-web/src/screens/find_detail.js`
   before flipping any feature flag.
5. Author a service-role RPC for AI-identification writes so
   `resolution_link` stays authoritative for new observations.
6. Only after (1)–(5) land can a client-cutover release be considered.

Each of those requires its own review and its own W3-A-style local
rehearsal. Do not shortcut.

## Safety

* production access: **no** — the installer refuses `--production` and
  the schema drafts are outside `supabase/migrations/`;
* production writes: **no**;
* client cutover: **no** — legacy taxonomy columns remain the
  authoritative reads for every client screen during W3-A;
* legacy-field removal: **no**;
* real manifest committed: **no** — the reconciliation manifest stays
  outside Git; only aggregate evidence is committed.
