import subprocess,json,sys
from pathlib import Path
root=Path('/Users/sigmundas/Documents/Code/sporely/sporely-web');S=Path.home()/'sporely-scratch/vernacular-2026-10-07-stage4b'
def db(sql):
 p=subprocess.run(['docker','exec','-i','supabase_db_zkpjklzfwzefhjluvhfw','psql','-X','-q','-v','ON_ERROR_STOP=1','-U','postgres','-d','postgres','-Atf','-'],input=sql,text=True,capture_output=True)
 if p.returncode:raise RuntimeError(p.stderr)
 return p.stdout
# All changes in local container and rolled back. Exact frozen SQL is not loaded or modified.
historical=(root/'supabase/migrations/20260724130000_add_taxonomy_v2_schema_and_search.sql').read_text();a=historical.index('create function public.taxonomy_v2_validate_release(');b=historical.index('\n$$;',a)+4
old=historical[a:b].replace('public.taxonomy_v2_validate_release','pg_temp.old_validate_release')
migration=(root/'supabase/migrations/20261007123912_fix_taxonomy_v2_parent_reference_plan.sql').read_text()
fixture="""INSERT INTO public.taxonomy_v2_releases(release_id,taxonomy_schema_version,export_schema_version,manifest_schema_version,exporter_version,scope_predicate_id,source_gz_sha256,source_sqlite_sha256,whole_export_sha256,manifest_sha256,generated_at,status,row_counts,authoritative_namespace_counts,legacy_source_counts,dangling_parent_count,dangling_parent_report,source_manifest) VALUES ('tax-2097.01.01-01',2,1,1,'fixture','fixture',repeat('a',64),repeat('b',64),repeat('d',64),repeat('c',64),now(),'ready','{"taxon.jsonl":52917,"scientific_name.jsonl":0,"vernacular.jsonl":0,"taxon_external_id.jsonl":0,"taxon_external_id_legacy_integer.jsonl":0,"taxon_redlist.jsonl":0}','{}','{}',0,'{}','{}');
INSERT INTO public.taxonomy_v2_concepts(sporely_taxon_id,first_seen_release_id) SELECT 910000000+i,'tax-2097.01.01-01' FROM generate_series(1,52917) i;
ANALYZE public.taxonomy_v2_taxa;
INSERT INTO public.taxonomy_v2_taxa(release_id,sporely_taxon_id,parent_sporely_taxon_id,canonical_source_system,canonical_external_id) SELECT 'tax-2097.01.01-01',910000000+i,CASE WHEN i=1 THEN NULL ELSE 910000001 END,'fixture',i::text FROM generate_series(1,52917) i;
"""
q="SELECT count(*) FROM public.taxonomy_v2_taxa t WHERE t.release_id='tax-2097.01.01-01' AND t.parent_sporely_taxon_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_taxa p WHERE p.release_id=%s AND p.sporely_taxon_id=t.parent_sporely_taxon_id%s)"
sql='BEGIN; SET LOCAL statement_timeout=\'2min\'; SET LOCAL plan_cache_mode=force_custom_plan;\n'+old+'\n'+migration+'\n'+fixture
sql+='EXPLAIN (ANALYZE,BUFFERS,VERBOSE,FORMAT JSON) '+q%('t.release_id','')+';\n'
sql+='EXPLAIN (ANALYZE,BUFFERS,VERBOSE,FORMAT JSON) '+q%('t.release_id',' OFFSET 0')+';\n'
sql+="EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT pg_temp.old_validate_release('tax-2097.01.01-01');\nEXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT public.taxonomy_v2_validate_release('tax-2097.01.01-01');\nROLLBACK;"
(S/'local-measure.sql').write_text(sql)
out=db(sql);(S/'local-measure.out').write_text(out);print(out[:18000])
