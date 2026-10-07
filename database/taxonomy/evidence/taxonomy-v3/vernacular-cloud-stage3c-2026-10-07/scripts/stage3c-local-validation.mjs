// Stage 3C (2026-10-07): execute the approved sporely-web importer payload in the LOCAL
// Supabase container only (docker exec psql, unix socket), with COMMIT replaced by
// probes + ROLLBACK. Derived from vernacular-refreeze-stage3-2026-10-06/scripts/local-validation.mjs;
// adds per-probe vernacular rows, qualified-name checks and content fingerprints.
// Usage (cwd = sporely-web): node stage3c-local-validation.mjs <run_dir>
import {readFileSync, writeFileSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {discoverLocalTarget, query, queryStdin} from '/Users/sigmundas/Documents/Code/sporely/sporely-web/scripts/taxonomy-v2/lib/docker-psql.mjs';

const root = process.argv[2];
const RID = 'tax-2026.10.06-01';
if (process.env.DATABASE_URL) throw Error('refuse: DATABASE_URL set; local container only');
const target = await discoverLocalTarget('/Users/sigmundas/Documents/Code/sporely/sporely-web');
const cname = execFileSync('docker', ['inspect', '-f', '{{.Name}}', target.container]).toString().trim();
if (cname !== '/supabase_db_zkpjklzfwzefhjluvhfw') throw Error('unexpected container ' + cname);
const host = await query(target, "select coalesce(inet_server_addr()::text,'unix-socket')||'|'||current_setting('listen_addresses')||'|'||current_setting('data_directory')");
const stateSql = "select json_build_object('releases',(select count(*) from public.taxonomy_v2_releases),'taxa',(select count(*) from public.taxonomy_v2_taxa),'vernacular',(select count(*) from public.taxonomy_v2_vernacular_names),'scientific_names',(select count(*) from public.taxonomy_v2_scientific_names),'external_ids',(select count(*) from public.taxonomy_v2_external_ids),'import_runs',(select count(*) from public.taxonomy_v2_import_runs),'observations',(select count(*) from public.observations))::text";
const before = await query(target, stateSql);
const probes = JSON.parse(readFileSync(root + '/probes-input.json', 'utf8'));
const checks = JSON.parse(readFileSync(root + '/dbchecks-input.json', 'utf8'));
const ids = JSON.parse(readFileSync(root + '/dbtaxa-input.json', 'utf8'));
const lit = s => "'" + String(s).replaceAll("'", "''") + "'";
const R = `release_id='${RID}'`;
let tail = `\nSET LOCAL statement_timeout='300s';\nSELECT 'STAGE3_COUNTS|' || json_build_object('taxa',(select count(*) from public.taxonomy_v2_taxa where ${R}),'scientific_names',(select count(*) from public.taxonomy_v2_scientific_names where ${R}),'vernacular',(select count(*) from public.taxonomy_v2_vernacular_names where ${R}),'external_ids',(select count(*) from public.taxonomy_v2_external_ids where ${R}),'legacy_external_ids',(select count(*) from public.taxonomy_v2_legacy_external_ids where ${R}),'redlist',(select count(*) from public.taxonomy_v2_redlist where ${R}),'active',(select count(*) from public.taxonomy_v2_releases where ${R} and status='active'))::text;\n`;
// Content fingerprints (C collation; reproduced byte-for-byte in stage3c_analyze.py).
tail += `SELECT 'STAGE3_FP|vernacular|' || md5(string_agg(sporely_taxon_id||'|'||language_code||'|'||vernacular_name||'|'||is_preferred_name::text||'|'||coalesce(source,''), E'\\n' ORDER BY sporely_taxon_id, language_code COLLATE "C", vernacular_name COLLATE "C")) FROM public.taxonomy_v2_vernacular_names WHERE ${R};\n`;
tail += `SELECT 'STAGE3_FP|taxa|' || md5(string_agg(sporely_taxon_id||'|'||coalesce(parent_sporely_taxon_id::text,'')||'|'||canonical_source_system||'|'||canonical_external_id||'|'||canonical_scientific_name, E'\\n' ORDER BY sporely_taxon_id)) FROM public.taxonomy_v2_taxa WHERE ${R};\n`;
tail += `SELECT 'STAGE3_FP|external_ids|' || md5(string_agg(sporely_taxon_id||'|'||source_system||'|'||namespace||'|'||external_id||'|'||id_role||'|'||is_preferred::text, E'\\n' ORDER BY sporely_taxon_id, source_system COLLATE "C", namespace COLLATE "C", external_id COLLATE "C", id_role COLLATE "C")) FROM public.taxonomy_v2_external_ids WHERE ${R};\n`;
tail += 'SET LOCAL ROLE authenticated;\nSELECT \'STAGE3_ROLE|\' || current_user;\n';
for (const p of probes) {
  tail += `SELECT 'STAGE3_PROBE|' || json_build_object('query',${lit(p.query)},'language','no','results',(select coalesce(jsonb_agg(to_jsonb(s)),'[]'::jsonb) from public.search_taxa_v2(${lit(p.query)},'no',50) s))::jsonb::text;\n`;
}
tail += 'RESET ROLE;\n';
for (const p of probes) {
  tail += `SELECT 'STAGE3_VROWS|' || json_build_object('query',${lit(p.query)},'rows',(select coalesce(json_agg(json_build_array(v.sporely_taxon_id,v.language_code,v.vernacular_name,v.is_preferred_name,v.source) ORDER BY v.sporely_taxon_id,v.language_code,v.vernacular_name),'[]'::json) from public.taxonomy_v2_vernacular_names v where v.${R} and v.language_code in ('nb','nn','no') and left(lower(v.vernacular_name), char_length(lower(${lit(p.query)}))) = lower(${lit(p.query)})))::jsonb::text;\n`;
}
for (const c of checks) {
  tail += `SELECT 'STAGE3_DBCHECK|' || json_build_object('id',${lit(c.id)},'rows',(select coalesce(json_agg(json_build_array(v.sporely_taxon_id,v.language_code,v.vernacular_name,v.is_preferred_name,v.source) ORDER BY v.sporely_taxon_id,v.language_code,v.vernacular_name),'[]'::json) from public.taxonomy_v2_vernacular_names v where v.${R} AND v.vernacular_name=${lit(c.name)}))::jsonb::text;\n`;
}
tail += `SELECT 'STAGE3_TAXVERN|' || json_build_object('ids',(select coalesce(json_agg(sporely_taxon_id ORDER BY sporely_taxon_id),'[]'::json) from public.taxonomy_v2_taxa where ${R} and sporely_taxon_id in (${ids.join(',')})),'nordic',(select coalesce(json_agg(json_build_array(v.sporely_taxon_id,v.language_code,v.vernacular_name,v.is_preferred_name,v.source) ORDER BY v.sporely_taxon_id,v.language_code,v.vernacular_name),'[]'::json) from public.taxonomy_v2_vernacular_names v where v.${R} and v.language_code in ('nb','nn','no') and v.sporely_taxon_id in (${ids.join(',')})))::text;\n`;
let sql = readFileSync(root + '/import.sql', 'utf8');
if (!sql.trimEnd().endsWith('COMMIT;')) throw Error('unexpected transaction end');
sql = sql.replace(/COMMIT;\s*$/, tail + '\nROLLBACK;\n');
writeFileSync(root + '/validation-rollback.sql', sql);
const output = await queryStdin(target, sql);
writeFileSync(root + '/local-validation.log', output);
const after = await query(target, stateSql);
if (before !== after) throw Error('local state did not roll back');
writeFileSync(root + '/rollback-verification.json', JSON.stringify({target, container_name: cname, server: host, database_url_env_present: false,
  before: JSON.parse(before), after: JSON.parse(after), transaction_rolled_back: true,
  rollback_sql_sha256: createHash('sha256').update(sql).digest('hex')}, null, 2) + '\n');
console.log('local import validated and rolled back', before);
