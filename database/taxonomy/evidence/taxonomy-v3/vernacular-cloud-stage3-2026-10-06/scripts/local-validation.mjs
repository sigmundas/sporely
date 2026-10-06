// Stage 3 rerun (2026-10-06): execute the approved importer payload in the LOCAL
// Supabase container only, inside its own transaction, with COMMIT replaced by
// probes + ROLLBACK. Derived from the 2026-10-04 local-validation.mjs.
// Usage (cwd = sporely-web): node local-validation.mjs <run_dir>
import {readFileSync, writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {discoverLocalTarget, query, queryStdin} from '/Users/sigmundas/Documents/Code/sporely/sporely-web/scripts/taxonomy-v2/lib/docker-psql.mjs';

const root = process.argv[2];
const RID = 'tax-2026.10.04-01';
if (process.env.DATABASE_URL) throw Error('refuse: DATABASE_URL set; local container only');
const target = await discoverLocalTarget('/Users/sigmundas/Documents/Code/sporely/sporely-web');
const host = await query(target, "select coalesce(inet_server_addr()::text,'unix-socket')||'|'||current_setting('listen_addresses')||'|'||current_setting('data_directory')");
const stateSql = "select json_build_object('releases',(select count(*) from public.taxonomy_v2_releases),'taxa',(select count(*) from public.taxonomy_v2_taxa),'vernacular',(select count(*) from public.taxonomy_v2_vernacular_names),'external_ids',(select count(*) from public.taxonomy_v2_external_ids),'import_runs',(select count(*) from public.taxonomy_v2_import_runs),'observations',(select count(*) from public.observations))::text";
const before = await query(target, stateSql);
const probes = JSON.parse(readFileSync(root + '/probes-input.json', 'utf8'));
const checks = JSON.parse(readFileSync(root + '/dbchecks-input.json', 'utf8'));
const lit = s => "'" + String(s).replaceAll("'", "''") + "'";
let tail = `\nSET LOCAL statement_timeout='180s';\nSELECT 'STAGE3_COUNTS|' || json_build_object('taxa',(select count(*) from public.taxonomy_v2_taxa where release_id='${RID}'),'scientific_names',(select count(*) from public.taxonomy_v2_scientific_names where release_id='${RID}'),'vernacular',(select count(*) from public.taxonomy_v2_vernacular_names where release_id='${RID}'),'external_ids',(select count(*) from public.taxonomy_v2_external_ids where release_id='${RID}'),'legacy_external_ids',(select count(*) from public.taxonomy_v2_legacy_external_ids where release_id='${RID}'),'redlist',(select count(*) from public.taxonomy_v2_redlist where release_id='${RID}'),'active',(select count(*) from public.taxonomy_v2_releases where release_id='${RID}' and status='active'))::text;\n`;
tail += 'SET LOCAL ROLE authenticated;\nSELECT \'STAGE3_ROLE|\' || current_user;\n';
for (const p of probes) {
  tail += `SELECT 'STAGE3_PROBE|' || json_build_object('query',${lit(p.query)},'language','no','expected_target',${p.expected_target ?? 'null'},'results',(select coalesce(jsonb_agg(to_jsonb(s)),'[]'::jsonb) from public.search_taxa_v2(${lit(p.query)},'no',50) s))::jsonb::text;\n`;
}
tail += 'RESET ROLE;\n';
for (const c of checks) {
  tail += `SELECT 'STAGE3_DBCHECK|' || json_build_object('id',${lit(c.id)},'rows',(select coalesce(json_agg(v ORDER BY v.sporely_taxon_id,v.language_code,v.vernacular_name),'[]'::json) from public.taxonomy_v2_vernacular_names v where v.release_id='${RID}' AND v.vernacular_name=${lit(c.name)}))::jsonb::text;\n`;
}
tail += `SELECT 'STAGE3_TAXON|' || json_build_object('ids',(select coalesce(json_agg(sporely_taxon_id ORDER BY sporely_taxon_id),'[]'::json) from public.taxonomy_v2_taxa where release_id='${RID}' and sporely_taxon_id in (96276,625268,100280,146835,621074,621443)))::text;\n`;
let sql = readFileSync(root + '/import.sql', 'utf8');
if (!sql.trimEnd().endsWith('COMMIT;')) throw Error('unexpected transaction end');
sql = sql.replace(/COMMIT;\s*$/, tail + '\nROLLBACK;\n');
writeFileSync(root + '/validation-rollback.sql', sql);
const output = await queryStdin(target, sql);
writeFileSync(root + '/local-validation.log', output);
const after = await query(target, stateSql);
if (before !== after) throw Error('local state did not roll back');
writeFileSync(root + '/rollback-verification.json', JSON.stringify({target, server: host, database_url_env_present: false,
  before: JSON.parse(before), after: JSON.parse(after), transaction_rolled_back: true,
  rollback_sql_sha256: createHash('sha256').update(sql).digest('hex')}, null, 2) + '\n');
console.log('local import validated and rolled back');
