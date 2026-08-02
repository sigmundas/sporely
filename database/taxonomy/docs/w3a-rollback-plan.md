# W3-A rollback plan

Two independent rollback modes are supported. Both were **proven locally**
against the disposable rehearsal stack (see
`w3a-rehearsal.test.mjs` integration cases).

## Mode 1 — Before client cutover (the default state during W3-A)

At this stage the additive column
`public.observations.resolved_sporely_taxon_id` may exist but no client
reads it. Legacy taxonomy/name columns remain the authoritative reads.

**Rollback action (single transaction, service-role only):**

```sql
-- 1. Drop everything W3-A added under public.observations.
drop trigger  if exists w3a_guard_resolved_sporely_taxon_id_trg on public.observations;
drop function if exists _w3a_guard_resolved_sporely_taxon_id();
alter table public.observations drop constraint if exists w3a_new_column_nullable;
alter table public.observations drop column if exists resolved_sporely_taxon_id;

-- 2. Discard the taxonomy_v3 schema entirely.
drop schema if exists taxonomy_v3 cascade;
```

**What is preserved:**

* every observation's original genus/species/common_name/artsdata_id/
  artportalen_id/inaturalist_id/mushroomobserver_id/desktop_id/
  ai_selected_* column, byte-identical to the state before W3-A;
* the pinned macrofungi release directory
  `tax-2026.08.01-01` and both supplements
  (`tax-2026.08.02-02`, `tax-2026.08.03-02`) on operator disk;
* prior evidence files, unchanged.

**What is lost:**

* the reconciliation-manifest audit row for
  `1beaa33f3891b216d3bc7c6d34cd96df1a936627c5a6f749a515cc75d51c094e`
  (may be reinstalled from the offline manifest at any time);
* the 369 identification snapshots and 369 resolution links;
* materialised registry_concept + external_mapping rows (all
  recomputable by re-running the installer).

**Local proof:** `integration: external-mapping conflict causes full
rollback` and every idempotency test remove all rows deterministically
after a schema drop and reinstall.

## Mode 2 — After a future client cutover (NOT scoped to W3-A)

At cutover time clients begin preferring the canonical link when
non-null. Rolling back after cutover requires:

**Rollback action:**

```sql
-- 1. Freeze W3-A writes.
revoke execute on function taxonomy_v3.install_release_chain(jsonb,jsonb,jsonb) from service_role;
revoke execute on function taxonomy_v3.link_observations_to_resolution() from service_role;

-- 2. Set every observation's canonical link to NULL so client fallback
--    is unambiguous. The historical taxonomy columns remain intact.
update public.observations set resolved_sporely_taxon_id = null
  where resolved_sporely_taxon_id is not null;

-- 3. Keep taxonomy_v3 schema installed for audit purposes. Do NOT drop
--    release_installation, supplement_installation, identification_snapshot
--    or reconciliation_manifest_audit — they remain the historical record
--    of what was installed and when.
--
-- If a hard rollback is required (contract violation), fall back to
-- Mode 1 after coordinating with all consumers.
```

**Compatibility during Mode 2:** clients must gracefully fall back to
the legacy columns when the canonical link is NULL — which was the
default state during W3-A. No historical snapshot data is lost.

## Local proof matrix

| failure mode | verified locally | test |
|---|---|---|
| partial release installation failure (chain interrupted mid-supplement) | ✓ (single-transaction `install_release_chain` — any raise rolls back everything installed in the call) | `integration: external-mapping conflict causes full rollback` |
| external mapping conflict | ✓ | same test — deliberately clashing tuple triggers the W2E-A2 invariant, zero orphan rows |
| bad manifest hash | ✓ (manifest audit's PK-on-SHA rejects a second install with the same SHA; a different SHA becomes a new audit row) | covered by idempotency case |
| bad supplement order | ✓ (installer refuses when a supplement's `depends_on` is not yet in `release_installation`) | supplement_installation FK to release_installation raises if depends_on missing |
| reconciliation failure midway | ✓ (single transaction; partial rows never survive) | rollback simulation exit code |
| repeated migration attempt (idempotent) | ✓ | `integration: install the release chain … Idempotency: second apply returns identical counts` |
| release-ID reuse with different bytes | ✓ | `integration: release-ID reuse with different hashes fails closed` |
