import subprocess,json,hashlib,sys,datetime
from pathlib import Path
R=Path('/Users/sigmundas/Documents/Code/sporely/sporely-web');S=Path.home()/'sporely-scratch/vernacular-2026-10-07-stage4b-deploy';phase=sys.argv[1]
env=Path.home()/'.config/sporely/production-db.env'
from urllib.parse import urlsplit
url=next(x.split('=',1)[1].strip().strip('"').strip("'") for x in env.read_text().splitlines() if x.startswith('DATABASE_URL='));u=urlsplit(url)
assert 'zkpjklzfwzefhjluvhfw' in (u.username or '') or 'zkpjklzfwzefhjluvhfw' in (u.hostname or '')
def query(sql):
 p=subprocess.run(['docker','run','--rm','-i','--env-file',str(env),'postgres:17','sh','-c','psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -Atf -'],input='BEGIN READ ONLY;\n'+sql+'\nROLLBACK;\n',text=True,capture_output=True)
 if p.returncode:raise RuntimeError(p.stderr)
 return [json.loads(x) for x in p.stdout.splitlines() if x]
tables=['public.taxonomy_v2_releases','public.taxonomy_v2_concepts','public.taxonomy_v2_taxa','public.taxonomy_v2_vernacular_names','public.taxonomy_v2_scientific_names','public.taxonomy_v2_external_ids','public.taxonomy_v2_legacy_external_ids','public.taxonomy_v2_redlist','public.taxonomy_v2_import_runs','taxonomy_v3.registry_concept','taxonomy_v3.external_mapping','taxonomy_v3.identification_snapshot','taxonomy_v3.resolution_link','taxonomy_v3.release_installation','taxonomy_v3.supplement_installation','taxonomy_v3.reconciliation_manifest_audit']
parts=[]
for table in tables:
 parts.append("'%s',(SELECT jsonb_build_object('count',count(*),'hash',md5(coalesce(string_agg(to_jsonb(t)::text,E'\\n' ORDER BY to_jsonb(t)::text),''))) FROM %s t)"%(table,table))
sql="SELECT jsonb_build_object('time',clock_timestamp(),'active',(SELECT jsonb_agg(release_id ORDER BY release_id) FROM public.taxonomy_v2_releases WHERE status='active'),'active_vernaculars',(SELECT count(*) FROM public.taxonomy_v2_vernacular_names WHERE release_id='tax-2026.09.30-01'),'target',(SELECT count(*) FROM public.taxonomy_v2_releases WHERE release_id='tax-2026.10.07-01'),'target_runs',(SELECT count(*) FROM public.taxonomy_v2_import_runs WHERE release_id='tax-2026.10.07-01'),'migration',(SELECT count(*) FROM supabase_migrations.schema_migrations WHERE version='20261007123912'),'function',(SELECT jsonb_build_object('definition',pg_get_functiondef(oid),'source',prosrc,'owner',proowner,'acl',proacl::text,'settings',proconfig,'oid',oid,'security_definer',prosecdef,'volatility',provolatile) FROM pg_proc WHERE oid='public.taxonomy_v2_validate_release(text)'::regprocedure),'timeout',current_setting('statement_timeout'),'indexes',(SELECT jsonb_agg(indexdef ORDER BY indexname) FROM pg_indexes WHERE schemaname='public' AND tablename LIKE 'taxonomy_v2_%'),'validation',public.taxonomy_v2_validate_release('tax-2026.09.30-01'),'invariants',jsonb_build_object("+','.join(parts)+"));"
a=query(sql)[0];(S/(phase+'.json')).write_text(json.dumps(a,indent=2,ensure_ascii=False));assert a['active']==['tax-2026.09.30-01'] and a['active_vernaculars']==13760 and a['invariants']['public.taxonomy_v2_concepts']['count']==52917 and a['target']==a['target_runs']==0 and a['timeout']=='2min' and a['validation']['ok']
if phase=='before':
 s=(R/'supabase/migrations/20260724130000_add_taxonomy_v2_schema_and_search.sql').read_text();s=s[s.index('create function public.taxonomy_v2_validate_release('):];expected=s.split('as $$',1)[1].split('$$;',1)[0]
 assert a['function']['source']==expected,'production validator differs from reviewed historical source'
 assert a['migration']==0
else:
 b=json.loads((S/'before.json').read_text());s=(R/'supabase/migrations/20261007123912_fix_taxonomy_v2_parent_reference_plan.sql').read_text();expected=s.split('as $$',1)[1].split('$$;',1)[0]
 assert a['function']['source']==expected,'deployed validator differs from reviewed migration'
 assert a['migration']==1 and a['invariants']==b['invariants'] and a['indexes']==b['indexes'] and a['validation']==b['validation'],'production invariant mismatch'
 assert {k:v for k,v in a['function'].items() if k not in ['definition','source']}=={k:v for k,v in b['function'].items() if k not in ['definition','source']},'function security/identity changed'
print(phase,'PASS',a['time'],'active',a['active'],'migration',a['migration'],flush=True)
if phase=='after':
 q="SELECT count(*) FROM public.taxonomy_v2_taxa t WHERE t.release_id='tax-2026.09.30-01' AND t.parent_sporely_taxon_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_taxa p WHERE p.release_id=t.release_id AND p.sporely_taxon_id=t.parent_sporely_taxon_id OFFSET 0)"
 # JSON EXPLAIN spans multiple lines; use psql unaligned JSON as raw output.
 for label,stmt in [('parent-plan','EXPLAIN (ANALYZE,BUFFERS,VERBOSE,FORMAT JSON) '+q),('validator-timing',"EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT public.taxonomy_v2_validate_release('tax-2026.09.30-01')")]:
  p=subprocess.run(['docker','run','--rm','-i','--env-file',str(env),'postgres:17','sh','-c','psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -Atf -'],input='BEGIN READ ONLY;\n'+stmt+';\nROLLBACK;\n',text=True,capture_output=True);assert p.returncode==0,p.stderr
  data=json.loads(p.stdout);(S/(label+'.json')).write_text(json.dumps(data,indent=2));print(label,data[0]['Execution Time'],'ms',flush=True)
  if label=='parent-plan':assert 'p.sporely_taxon_id = t.parent_sporely_taxon_id' in p.stdout and 'p.release_id = t.release_id' in p.stdout and 'Join Filter' not in p.stdout
