# Production import — tax-2026.10.31-01 (2026-11-04)

Executed under separate owner GOs on 2026-11-04, following `import-runbook.md`.
Target: project `zkpjklzfwzefhjluvhfw`, system identifier `7606130613603586966`.

1. **Migration** `20261031120000_accept_name_association_national_names.sql`:
   sporely-web deploy tree at `ecfe6b6` (= `origin/main`); `check` listed exactly
   this version; `supabase db push --linked`; `post-verify`: remote matches the
   deploy tree, deferred `20260914090000` still absent. `-11` stayed active,
   validates, and has no national-name errors.
2. **Preflight** (read-only, `preflight.sql`) before and after the migration and
   again immediately before the import: identical apart from the three migration
   lines; `-11` counts 53,166 / 61,797 / 61,157 / 57,771 / 2,984; 0 selected
   concepts outside the active release (237 distinct, all in `10.31-01`).
3. **Import and activation**: `tax-2026.10.31-01-import.sql`
   (`227b443364c2e9bdb84248d98205488aecb250f3c3e8a18b1935fa6eb3da28d1`), one transaction,
   2026-11-04T18:35:31Z – 2026-11-04T18:36:39Z, exit 0, COMMIT.
4. **Post-import verification** (read-only): `import-post-verify.sql` /
   `import-post-verify.out`. `10.31-01` active, `-11` retired with its rows;
   validates; no national-name errors; import run succeeded with the planned
   counts (53,166 / 66,718 / 62,025 / 77,451 / 3,601); Norwegian search probes and
   COL lookups unchanged from `-11`; NorTaxa 226896 and Dyntaxa 3213 resolve
   through name associations; 0 selected concepts outside the active release.

**Writers were not paused.** Runbook step 1 (pausing admin tooling, repair jobs
and service-role scripts, and asking users to pause syncing) was not performed.
The import's own cutover locks and its selected-concept check ran inside the
transaction, and the check after the import found 0 selected concepts outside the
active release. No browser smoke test of the web picker was run.

No further production database changes are authorized.
