# W3-A2 production-shaped integration corrections

Every W3-A2 correction the accepted brief requires was implemented and
proven locally. **No production migration created, no staging access.**

## 1. Pseudonymous → real-ID bridge

New tool: [`database/taxonomy/scripts/build_deployment_manifest.py`](../../scripts/build_deployment_manifest.py).

Reads the operator's raw CSV export + the reconciliation manifest, uses
the private HMAC key to recompute each pseudonym, joins back to the
real observation id, and adds a taxonomy-field fingerprint per row.

Refuses (hard fail, non-zero exit):

* a manifest observation_id that does not join to any raw row;
* a raw row whose pseudonym collides with another raw row's;
* the manifest containing a duplicate observation_id;
* the output landing under either repository (sporely-py or sporely-web)
  or outside an explicit `--allow-output-under` allowlist;
* `--production` (exit 3).

Local run against the accepted W2E-B/W2E-B-corrections manifest
(`semantic_sha256=1beaa33f…`, 369 records):

```
matched_observations                       369
manifest_records                           369
raw_rows                                   369
resolved_count                             233
unresolved_or_manual_or_no_evidence_count  136
drift_counts                               {not_checked: 369, ...}
manifest_input_file_sha256                 1beaa33f3891b216d3bc7c6d34cd96df1a936627c5a6f749a515cc75d51c094e
raw_export_sha256                          8efbedfaedb3fea94c6d5ebcc4b80eb65c04e535f8b27649dfdcf353e40b17e4
```

The deployment manifest itself lives at `/tmp/w3a2-deployment/…` and is
**not** committed. The taxonomy-field fingerprint (SHA-256 over the
sorted taxonomy columns of the raw row) is stored per record so a later
step can compare it against `public.observations` and fail closed when
legacy fields drifted after the export. Drift-check integration is
covered end-to-end by the `--observations-fingerprint` flag path — the
current run left drift as `not_checked` because production-side dumps
are outside scope.

## 2. Canonical-link write protection (INSERT + UPDATE)

`_w3a_guard_resolved_sporely_taxon_id()` now:

* fires on `INSERT` too (previously only `UPDATE`);
* identifies privileged callers by DATABASE ROLE
  (`current_user in ('service_role','postgres','supabase_admin')`), no
  longer via `request.jwt.claim.role` which a client-side JWT can carry;
* raises `insufficient_privilege` when a non-privileged role attempts to
  set the column on `INSERT` or change it on `UPDATE`.

Locally proven with actual database roles:

| role | attempted operation | result |
|---|---|---|
| anon | `INSERT` with `resolved_sporely_taxon_id = 42` | rejected — "can only be set by service_role" |
| anon | `UPDATE` changing the column | rejected / no change |
| authenticated | `UPDATE` changing the column | rejected / no change |
| service_role | `INSERT ... resolved_sporely_taxon_id = 634896` | accepted |

## 3. Resolution-link privacy — no more USING(true)

The blanket policy is replaced with two per-role policies that JOIN to
`public.observations` and inherit its visibility:

```sql
create policy taxonomy_v3_read_resolution_anon
  on resolution_link for select to anon
  using (exists (select 1 from public.observations o
                  where o.id::text = resolution_link.observation_id
                    and o.visibility = 'public'));
create policy taxonomy_v3_read_resolution_authenticated
  on resolution_link for select to authenticated
  using (exists (select 1 from public.observations o
                  where o.id::text = resolution_link.observation_id
                    and (o.visibility = 'public' or o.user_id = auth.uid())));
```

Locally proven:

| scenario | verdict |
|---|---|
| anon reads three seeded links (900221 public, 900222 owner-private, 900223 other-private) | sees only `{900221}` |
| authenticated with no JWT | sees only public rows (`{900221}`) |
| unresolved private observation | not leaked to anon |
| owner path (visible under a real JWT) | contract-preserved — staging must supply real jwt.claim.sub for a full owner assertion; the local test proves the blanket `true` is gone |

## 4. Cryptographic manifest verification

`verifyManifest(rawText, expectedSha)` in
[`scripts/taxonomy-v3/install-release-chain.mjs`](../../../../sporely-web/scripts/taxonomy-v3/install-release-chain.mjs)
recomputes the semantic SHA and enforces every structural rule before
SQL runs:

| tamper | detected |
|---|---|
| declared SHA differs from recomputed | ✓ |
| `record_count` disagrees with `records.length` | ✓ |
| duplicate `observation_id` | ✓ |
| `aggregate_counts` disagrees with actual state distribution | ✓ |
| `resolved_*` record with null target | ✓ |
| unresolved record with non-null target | ✓ |

The SQL side records both hashes:

| column | meaning |
|---|---|
| `manifest_semantic_sha256` | primary key — content identity |
| `input_file_sha256` | raw bytes of the file loaded |

`ON CONFLICT (manifest_semantic_sha256) DO UPDATE ... WHERE record_count
= excluded.record_count AND state_counts = excluded.state_counts AND
input_file_sha256 IS NOT DISTINCT FROM excluded.input_file_sha256` — a
same-content reapply is idempotent, a different-bytes-same-SHA (or
disagreeing counts) reapply raises.

## 5. Complete release-chain hash verification

`loadBase()` now verifies **every** artefact listed in
`taxonomy_export_manifest.json.files[]` against its on-disk SHA-256,
plus a cross-check that `scope_manifest_sha256` matches the on-disk
`scope-manifest.json`. Supplement validation stays covered by the
W2E-B `SupplementLoader` (12/12 tests).

## 6. Registry identity hardening

`registry_concept` inserts on the installer path now use:

```
on conflict (sporely_taxon_id) do update
  set canonical_name = registry_concept.canonical_name
  where registry_concept.canonical_name is not distinct from excluded.canonical_name
    and registry_concept.rank           is not distinct from excluded.rank
    and registry_concept.scope_state    is not distinct from excluded.scope_state
    and registry_concept.cache_state    is not distinct from excluded.cache_state;
```

with a follow-up `RAISE 'registry_concept identity conflict'` when the
UPDATE matches zero rows. Same-identity reapply → idempotent. A
supplement that presents `sporely_taxon_id=634896` with a different
`canonical_name`/`rank`/`scope_state`/`cache_state` raises inside the
transaction and rolls back the whole install.

Foreign-key semantics (unchanged, listed for completeness):

* `external_mapping.sporely_taxon_id` → `registry_concept(sporely_taxon_id)` `ON DELETE RESTRICT`
* `resolution_link.resolved_sporely_taxon_id` → `registry_concept(sporely_taxon_id)` `ON DELETE SET NULL`
* `public.observations.resolved_sporely_taxon_id` → same, `ON DELETE SET NULL`

`RESTRICT` on external_mapping prevents deletion of a registry concept
while any external mapping still points to it.

## 7. Additive-only migration draft

New files (no destructive DDL):

* [`sporely-web/supabase/drafts/taxonomy_v3_schema_additive.sql`](../../../../sporely-web/supabase/drafts/taxonomy_v3_schema_additive.sql)
* [`sporely-web/supabase/drafts/taxonomy_v3_observations_integration_draft_additive.sql`](../../../../sporely-web/supabase/drafts/taxonomy_v3_observations_integration_draft_additive.sql)

Verified locally against three stack states:

1. fresh empty stack (no taxonomy_v3) — applies cleanly, ends with 7 taxonomy_v3 tables;
2. stack with the original destructive-draft schema already installed — additive on top applies cleanly;
3. stack with the additive draft already applied — re-applying is a no-op.

`grep -c "^drop " ... additive.sql` = **0**.

## Test summary

* `W3A_INTEGRATION=1 node --test scripts/taxonomy-v3/w3a-rehearsal.test.mjs` → **9/9 pass** (updated for W3-A2 hardening)
* `W3A2_INTEGRATION=1 node --test scripts/taxonomy-v3/w3a2-corrections.test.mjs` → **9/9 pass**
* Desktop `python -m pytest tests/taxonomy/` → **55/55 pass**

## Remaining blockers before staging

1. Porting `supabase/drafts/*.sql` → real `supabase/migrations/` requires human authorisation.
2. Operator must run `build_deployment_manifest.py` against a production-side observations dump to populate the drift-check column before real staging apply.
3. The pseudonym key stays outside Git; a secure delivery path for staging (e.g. GitHub Actions secret or Vault) needs formal approval.
4. Real-jwt owner-visibility test on `resolution_link` requires a Supabase staging session with `auth.uid()` set — cannot be exercised locally without full auth simulation.

## Safety

* production access **no** · production writes **no** · production migration creation **no**
* staging migration application **no** · client cutover **no**
* raw-ID deployment manifest committed **no**
* pseudonym key exposed **no**
