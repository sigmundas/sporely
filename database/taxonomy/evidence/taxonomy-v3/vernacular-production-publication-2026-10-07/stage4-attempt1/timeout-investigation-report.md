# VALIDATOR_FIX_REQUIRED

Investigation only, 2026-10-07. No import retry, production DML/DDL, ANALYZE, migration repair or artifact regeneration. Production queries ran in READ ONLY transactions, ending in ROLLBACK. Scoped settings were used only for read-only plan/proof tests and reverted.

## Exact failing query

```sql
select count(*) into v_dangling
from public.taxonomy_v2_taxa t
where t.release_id = p_release_id
  and t.parent_sporely_taxon_id is not null
  and not exists (
    select 1 from public.taxonomy_v2_taxa p
    where p.release_id = t.release_id
      and p.sporely_taxon_id = t.parent_sporely_taxon_id
  );
```

`INTO v_dangling` is PL/pgSQL assignment; the standalone EXPLAIN query omits only that assignment. Purpose: count same-release missing parents, then require equality with the immutable declared dangling_parent_count (one expected boundary parent in this scoped candidate). Never skip or replace this with a blanket zero-parent assertion.

Source: deployed public.taxonomy_v2_validate_release(text), created in sporely-web/supabase/migrations/20260724130000_add_taxonomy_v2_schema_and_search.sql:314. Failure context function line 68. Frozen SQL calls it at import.sql:233204 inside DO $validate$, terminating at :233209; tooling source scripts/taxonomy-v2/prepare-production-release-import.mjs:363. This is pre-activation validation after load/count verification/ready transition. Activation subsequently calls this validator twice more. SQL does not set a timeout, force a plan, or ANALYZE newly loaded tables.

## Plans and measurements

- Original query on existing active 52,917-taxon release: merge right anti join; parent index + primary-key index-only scans. Estimates 52,334 rows per input; actual child scan 52,917, parent scan 52,902 (merge stops after largest child parent); missing-parent output one. 351.061 ms execution, 414 shared buffers, zero heap fetches/temp writes. Counts and expected parent boundary pass.
- Parameterized force_generic_plan on active release: same indexed merge anti join, estimates 52,491, 22.419 ms warm execution, one dangling parent. This is diagnostic evidence, not an approved importer setting.
- Exact target literal/custom prepared plan while target is absent: estimates ONE row in BOTH inputs. Nested-loop anti join. Inner index condition is ONLY release_id; parent equality is a join filter, not an index condition. With a freshly inserted release not in column statistics, a custom plan can retain this severe underestimate. Last autoanalyze 2026-09-30 20:59:48 UTC; release_id statistics contain only the three old releases. Both required indexes already exist.
- Frozen candidate has 52,917 nonnull parent references, one expected missing parent. If the observed release-only ascending inner index scan is used after load, exact frozen parent distribution implies 1,309,731,581 comparisons. This is a structural work estimate, NOT a measured duration or captured failed-session plan. The actual nested plan inside the failed transaction was not retained; no production reload was performed to capture it.
- Full existing-release validator: 2,142.401 ms. Thus two minutes is not the ordinary cost of this data volume.
- Read-only semantics-preserving query alternative with OFFSET 0 inside NOT EXISTS: missing-target EXPLAIN gives a correlated lookup whose primary-key index condition includes BOTH release_id and parent ID. Active-release execution: 189.654 ms, 52,917 index probes, same one dangling parent. This is a proposed plan-stability technique requiring a separate reviewed validator migration, not a production change made here.

Conclusion: evidence strongly supports an avoidable cardinality/plan pathology for a newly loaded release. Raising the timeout alone would merely permit repeated scans; no defensible finite retry timeout follows from the measurements. Recommend a separately reviewed validator/validation-execution fix, with a cold-statistics fresh-release test and all original count/parent/integrity checks preserved. Options for that review include a correlated indexed existence check, or a tightly bounded generic-plan execution policy. Statistics refresh is another tooling option but cannot be added by rewriting this frozen SQL. Do not edit the applied migration; deploy any function change through a new guarded migration. No edit made in this investigation.

## Timeout scope

Exact import connection role: postgres, database postgres, session-mode pooler port 5432. pg_settings reports statement_timeout=120000 ms, reset_val=120000, source=configuration file, /etc/postgresql-custom/platform-defaults.conf:6, context=user. This is the server baseline, not an import role/database/session override. Role/database settings contain no applicable timeout override; validate/activate proconfig sets only search_path.

Read-only proof: before 2min/configuration file; SET LOCAL statement_timeout='121s' gives 121s/session; after ROLLBACK returns 2min/configuration file. A new connection also retains 2min. Thus SET LOCAL can safely scope an eventual override to one transaction; session SET on a dedicated containerized psql connection is also possible, with RESET/connection disposal. Frozen SQL already begins and commits its own transaction. A SET LOCAL issued before its BEGIN would not apply; any future wrapper must account for that. Do not mutate frozen SQL or assume PGOPTIONS is accepted through the pooler without verifying. No timeout-only retry procedure/value is recommended under this verdict.

## Retry state / immutable artifacts

At 2026-10-07 12:34:39 UTC (14:34:39 Europe/Oslo): sole active tax-2026.09.30-01, 52,917 concepts, 13,760 active vernaculars, active validator ok/errors=[]; target release/import-run/taxa/scientific/external/legacy/vernacular/red-list rows all zero. No partial state to repair.

Exact preserved SQL: ~/sporely-scratch/vernacular-2026-10-07-f1/run1/import.sql, SHA-256 cc9a1ddfc4c5f56fa553935b79fb40a2eda01588f0c6d2781e243cddda84852e, unchanged. Freeze SHA-256 a86e35854fd4d01984d8b8fe121d8fc318e75a01876d243975847127462fd14e unchanged; verify_frozen passes all frozen files, members and SQLite bytes. No release regeneration is needed. The SQL calls the deployed validator rather than embedding its definition, so an independently reviewed validator correction could allow the SAME SQL to be reused later.

## Evidence

settings.out/sql; plan_target_absent.out; plan_active_measured.out; prepared_plans.out; full_validator_timing.out; candidate-plan-bound.json; candidate_nonflattened_plan.out; active_nonflattened_measured.out; timeout_scope_proof.out; retry_safety.out. Original attempt evidence remains adjacent in the Stage 4 directory.

No retry performed. No recommended timeout value for another attempt until the plan defect is addressed and fresh-release validation is measured. All artifacts and production taxonomy remain unchanged.
