# tax-2026.10.31-01 production import runbook (prepared 2026-11-04, NOT executed)

Release approved by the owner 2026-11-04 (`release-decision-tax-2026.10.31-01.json`, decision
`release_approved`, bound to the frozen artifact below). **Steps A and 4 are production writes and
each needs its own explicit owner go-ahead.** Nothing below has been run against production.

| Item | Value |
|---|---|
| SQLite | `ec19acac6a0dafbd3fd629760d9aa92fa5d7ec270597e29fffe72c7cba5cacfe` |
| Gzip | `3e13a125c3a3d5ec4b3fd7e26c1ba2aa0eaaa9f36b3e82ce3fa271f46f77b3de` |
| freeze.json | `3b9aaa9ce00fe17cb9f47224fed04e68669976658ecdcf726039ab681c7425a5` (derived from `tax-2026.10.23-11`, freeze `26e77446…bc99`) |
| Import SQL | `production/run1/tax-2026.10.31-01-import.sql`, sha256 `227b443364c2e9bdb84248d98205488aecb250f3c3e8a18b1935fa6eb3da28d1` (run2 identical) |
| Scope export manifest | `production/run1/global_macrofungi_tax-2026.10.31-01/taxonomy_export_manifest.json`, sha256 `da26931db1e29b0388113bcb2dfab1ccf33a42bc6503a5e5cb7025bf9bc5ba5a` |
| Generators | sporely-py `0c54c47` (export, scope); sporely-web `ecfe6b6` (main, PR #51 merged) |
| Required migration | sporely-web `20261031120000_accept_name_association_national_names.sql` |
| Production target | project ref `zkpjklzfwzefhjluvhfw`, system identifier `7606130613603586966` |

Export from the frozen gzip with the release decision: `export_use: release`, `importable: true`.
Data files identical to the 2026-10-31 candidate export (whole-export sha `88d9b055…416a`); the scope
output is identical to the rehearsed scope, and run1 equals run2 byte for byte.

```bash
REL=/Users/sigmundas/Documents/Code/sporely/releases/tax-2026.10.31-01
SQLD=$REL/production/run1
ENV=~/.config/sporely/production-db.env      # passed to docker only; never printed
RO() { docker run --rm -i --env-file $ENV postgres:17 sh -c 'psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -At -f -'; }
```

**Blocker, 2026-11-04:** `production-db.env` (modified 2026-11-03 15:12) no longer holds `DATABASE_URL`;
it contains unrelated notes, and docker refuses it. Restore it before steps 3–5.

## What changes (cloud scope, `-11` → `10.31-01`)

| Table | `-11` | `10.31-01` | Change |
|---|---|---|---|
| taxa / concepts | 53,166 | 53,166 | same id set; none added or removed |
| scientific_names | 61,797 | 66,718 | +4,921, none removed |
| vernacular_names | 61,157 | 62,025 | +868, none removed |
| external_ids | 57,771 | 77,451 | +19,680 `name_association` rows (14,592 NorTaxa, 5,088 Dyntaxa), none removed |
| redlist | 2,984 | 3,601 | +617, none removed |
| legacy_external_ids | 0 | 0 | — |

Taxon rows: Norwegian preferred scientific name gained on 7,150 taxa and Swedish on 5,088 (none lost
or changed); `inaturalist_taxon_id` gained on 831 (carried onto associated concepts, report counter
`inaturalist_column`); `sporely_content_release_id` changes on every row. The 36,779 associations are
the desktop-wide count; the cloud scope carries the 19,680 external-id rows above.

## A. Migration — PRODUCTION WRITE (separate explicit approval required)
Must precede step 4: the rehearsal showed the import refused without it. Additive: one function
(`taxonomy_v2_national_name_errors`) also accepts `name_association:%` provenance, plus a column comment.
In sporely-web, per its AGENTS.md (never `--include-all`, never `migration up --linked`, never MCP `apply_migration`):
```bash
node scripts/supabase-deploy-tree.mjs check      # deploy tree prepared at ecfe6b6, allowlist 20261031120000
# inside the deploy tree:
supabase db push --linked
node scripts/supabase-deploy-tree.mjs post-verify
```
Rollback: not needed for the import rollback (harmless with `-11` active); a revert would be a new forward migration.

## 1. Pause writers (operator) — no database change
As for `-11`: announce a ~1 min window; pause admin tooling, repair jobs and service-role scripts that
write `public.observations`, `taxonomy_v3.*` or the reference tables; ask users to pause syncing if possible.

## 2. Verify artefacts — LOCAL, read-only
```bash
cd $REL && shasum -a 256 -c SHA256SUMS --quiet
```
Expected: no output. Stop on any mismatch.

## 3. Read-only preflight — READ-ONLY, immediately before step 4
```bash
RO < $REL/release-prep/preflight-2026-11-04/preflight.sql
```
Expected: system_identifier `7606130613603586966`; rehearsal_marker_present false; active
`tax-2026.10.23-11`; r1031_rows all 0; r11_counts 53166 / 61797 / 61157 / 57771 / 2984;
migration_20261031120000_applied true and national_name_errors_accepts_name_association true (after A);
cg1_selected_not_in_active 0; every selected id present in the `10.31-01` scope (same id set as `-11`);
long transactions, idle-in-transaction and ungranted locks 0. Stop on any difference.

## 4. Import + activation — PRODUCTION WRITE (separate explicit approval required)
```bash
docker run --rm --env-file $ENV -v "$SQLD:/payload:ro" postgres:17 \
  sh -c 'psql "$DATABASE_URL" -X --file=/payload/tax-2026.10.31-01-import.sql'
echo "exit $?"
```
One transaction, same generator as `-11` (lock_timeout 10 s, statement_timeout 30 min; load and validate,
then the cutover under the activation lock, protected-state snapshot, activate, selected-concept check,
COMMIT). Expected: exit 0, seconds (the `-11` import took ~6 s). On failure the transaction rolls back
completely and `-11` stays active; rerun step 3, fix the cause, rerun the same SQL.

## 5. Post-import verification — READ-ONLY
```sql
BEGIN READ ONLY;
SELECT release_id, status FROM public.taxonomy_v2_releases WHERE release_id IN ('tax-2026.10.23-11','tax-2026.10.31-01');  -- -11 retired, 10.31-01 active
SELECT public.taxonomy_v2_validate_release('tax-2026.10.31-01')->>'ok';                                                       -- true
SELECT public.taxonomy_v2_national_name_errors('tax-2026.10.31-01');                                                          -- []
SELECT status, counts FROM public.taxonomy_v2_import_runs WHERE release_id='tax-2026.10.31-01';                              -- succeeded
SELECT (SELECT count(*) FROM public.taxonomy_v2_taxa WHERE release_id='tax-2026.10.31-01'),
       (SELECT count(*) FROM public.taxonomy_v2_external_ids WHERE release_id='tax-2026.10.31-01'),
       (SELECT count(*) FROM public.taxonomy_v2_redlist WHERE release_id='tax-2026.10.31-01');                                -- 53166, 77451, 3601
SELECT (SELECT taxon_id FROM public.search_taxa_v2(q,'no',5) LIMIT 1) FROM (VALUES ('kantareller'),('eggrøyksopper'),('fingersopper'),('fagerkjuker'),('beitemarkvokssopper'),('silkeparasollsopp'),('kalktalg')) v(q);
   -- unchanged from -11: 129742, 8992, 20056, 15031, 68411, 6398, 140472
SELECT (SELECT string_agg(taxon_id::text,',') FROM public.resolve_taxon_external_id_v2('col_xr','col_usage_id',c)) FROM (VALUES ('XQVS'),('XQVT')) v(c);  -- 616943, 616942
SELECT (SELECT string_agg(taxon_id::text,',') FROM public.resolve_taxon_external_id_v2('nortaxa','nortaxa_taxon_id','226896')),                -- 16187 (Hydnum repandum; unresolved before)
       (SELECT string_agg(taxon_id::text,',') FROM public.resolve_taxon_external_id_v2('dyntaxa','dyntaxa_taxon_id','urn:lsid:dyntaxa.se:Taxon:3213'));  -- 168873 (Cantharellus cibarius)
SELECT count(DISTINCT o.selected_sporely_taxon_id) FROM public.observations o WHERE o.selected_sporely_taxon_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_taxa t JOIN public.taxonomy_v2_releases r USING (release_id) WHERE r.status='active' AND t.sporely_taxon_id=o.selected_sporely_taxon_id);  -- 0
ROLLBACK;
```
Then a web smoke test of the picker without writes (search one name above; a Norwegian/Swedish
preferred scientific name shows).

## 6. Resume writers (operator)

## Rollback — PRODUCTION WRITE (separate approval)
```sql
BEGIN;
UPDATE public.taxonomy_v2_releases SET status='ready' WHERE release_id='tax-2026.10.23-11' AND status='retired';
SELECT public.taxonomy_v2_activate_release('tax-2026.10.23-11');
COMMIT;
```
`10.31-01` becomes retired with its rows kept. Both releases have the same 53,166 concept ids, so every
selection made under `10.31-01` still resolves under `-11` (rollback to `-11` was rehearsed on
2026-10-31). What is lost on rollback: the 19,680 association external ids stop resolving, and the
national names, extra names and 617 Red List rows disappear from search and display. Stored
observation identities are concept ids and are unaffected. The migration stays (harmless).

## Not part of this runbook
Desktop promotion (`promote_desktop_bundle.py --expect-freeze-sha256 3b9aaa9c… --release-decision …`),
the PR #53 merge and desktop publication: separate approvals.
