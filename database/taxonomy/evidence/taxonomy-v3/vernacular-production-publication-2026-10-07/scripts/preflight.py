import subprocess,json,hashlib,datetime
from pathlib import Path
from urllib.parse import urlsplit
S=Path.home()/'sporely-scratch/vernacular-2026-10-07-stage4c'
FR=Path.home()/'sporely-scratch/vernacular-2026-10-07-f1'
F=FR/'run1/scoped'
S4=Path.home()/'sporely-scratch/vernacular-2026-10-07-stage4'
B4=Path.home()/'sporely-scratch/vernacular-2026-10-07-stage4b-deploy/after.json'
WEB=Path('/Users/sigmundas/Documents/Code/sporely/sporely-web')
ENV=Path.home()/'.config/sporely/production-db.env'
REF='zkpjklzfwzefhjluvhfw';PREV='tax-2026.09.30-01';RID='tax-2026.10.07-01'
FREEZE='a86e35854fd4d01984d8b8fe121d8fc318e75a01876d243975847127462fd14e'
SQLH='cc9a1ddfc4c5f56fa553935b79fb40a2eda01588f0c6d2781e243cddda84852e'
PAYLOAD=FR/'run1/import.sql'
url=next(x.split('=',1)[1].strip().strip('"').strip("'") for x in ENV.read_text().splitlines() if x.startswith('DATABASE_URL='));u=urlsplit(url)
assert REF in (u.username or '') or REF in (u.hostname or ''),'env does not target production project'
def query(sql):
 p=subprocess.run(['docker','run','--rm','-i','--env-file',str(ENV),'postgres:17','sh','-c','psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -Atf -'],input='BEGIN READ ONLY;\n'+sql+'\nROLLBACK;\n',text=True,capture_output=True)
 if p.returncode:raise RuntimeError(p.stderr)
 return [json.loads(x) for x in p.stdout.splitlines() if x]
def canon(x):return json.dumps(x,sort_keys=True,ensure_ascii=False,separators=(',',':'))
def digest(rows):return hashlib.sha256(('\n'.join(sorted(canon(x) for x in rows))+'\n').encode()).hexdigest()
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
FP_TABLES=['public.taxonomy_v2_releases','public.taxonomy_v2_concepts','public.taxonomy_v2_taxa','public.taxonomy_v2_vernacular_names','public.taxonomy_v2_scientific_names','public.taxonomy_v2_external_ids','public.taxonomy_v2_legacy_external_ids','public.taxonomy_v2_redlist','public.taxonomy_v2_import_runs','taxonomy_v3.registry_concept','taxonomy_v3.external_mapping','taxonomy_v3.identification_snapshot','taxonomy_v3.resolution_link','taxonomy_v3.release_installation','taxonomy_v3.supplement_installation','taxonomy_v3.reconciliation_manifest_audit']
V3=['registry_concept','external_mapping','identification_snapshot','resolution_link','release_installation','supplement_installation','reconciliation_manifest_audit']
SPECS=[('taxon.jsonl','taxonomy_v2_taxa'),('scientific_name.jsonl','taxonomy_v2_scientific_names'),('taxon_external_id.jsonl','taxonomy_v2_external_ids'),('taxon_redlist.jsonl','taxonomy_v2_redlist')]
FN_SQL="(SELECT jsonb_build_object('source',prosrc,'owner',proowner,'acl',proacl::text,'settings',proconfig,'security_definer',prosecdef,'volatility',provolatile) FROM pg_proc WHERE oid='public.taxonomy_v2_validate_release(text)'::regprocedure)"
def expected_rows(file,table,keys):
 out=[]
 for line in (F/file).read_text().splitlines():
  x=json.loads(line);x['sporely_taxon_id']=x.pop('taxon_id')
  if table=='taxonomy_v2_taxa':x['parent_sporely_taxon_id']=x.pop('parent_taxon_id')
  if table=='taxonomy_v2_scientific_names':x['alias_reason']=x.pop('note')
  out.append({k:x.get(k) for k in keys})
 return out
def validator_expected():
 s=(WEB/'supabase/migrations/20261007123912_fix_taxonomy_v2_parent_reference_plan.sql').read_text();return s.split('as $$',1)[1].split('$$;',1)[0]
def fn_security(fn):return {k:fn[k] for k in ['owner','acl','settings','security_definer','volatility']}
def fingerprint():
 parts=["'%s',(SELECT jsonb_build_object('count',count(*),'hash',md5(coalesce(string_agg(to_jsonb(t)::text,E'\\n' ORDER BY to_jsonb(t)::text),''))) FROM %s t)"%(t,t) for t in FP_TABLES]
 return query("SELECT jsonb_build_object("+','.join(parts)+");")[0]
if __name__=='__main__':
 report={'start_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'project_ref':REF,'previous_release':PREV,'release_id':RID,'checks':{},'writes_occurred':False}
 def stop(label,detail=None):
  report['result']='STOP_PRODUCTION_MISMATCH';report['divergence']=label;report['detail']=detail
  (S/'preflight.json').write_text(json.dumps(report,indent=2,ensure_ascii=False,default=str));raise SystemExit('STOP: '+label+' '+json.dumps(detail,default=str)[:1500])
 fz=FR/'final/freeze.json'
 if sha(fz)!=FREEZE:stop('freeze hash',sha(fz))
 files=json.loads(fz.read_text())['files'];fh={}
 for n,m in files.items():
  p=FR/'final'/n;h=sha(p);fh[n]=h
  if h!=m['sha256'] or p.stat().st_size!=m['bytes']:stop('frozen file '+n,h)
 for line in (FR/'final.sha256').read_text().splitlines():
  h,n=line.split()
  if h!=sha(FR/'final'/n):stop('final.sha256 '+n)
 if sha(PAYLOAD)!=SQLH:stop('import SQL hash')
 if sha(FR/'run2/import.sql')!=SQLH:stop('run2 import SQL hash')
 report['checks']['artifacts']={'freeze_sha256':FREEZE,'frozen_files':fh,'import_sql':str(PAYLOAD),'import_sql_sha256':SQLH,'import_sql_bytes':PAYLOAD.stat().st_size,'run2_identical':True}
 print('artifacts PASS',flush=True)
 state=query(f"SELECT jsonb_build_object('time',clock_timestamp(),'releases',(SELECT jsonb_agg(jsonb_build_object('release_id',release_id,'status',status) ORDER BY release_id) FROM public.taxonomy_v2_releases),'active',(SELECT jsonb_agg(release_id ORDER BY release_id) FROM public.taxonomy_v2_releases WHERE status='active'),'concepts',(SELECT count(*) FROM public.taxonomy_v2_concepts),'prev_vernaculars',(SELECT count(*) FROM public.taxonomy_v2_vernacular_names WHERE release_id='{PREV}'),'target',(SELECT count(*) FROM public.taxonomy_v2_releases WHERE release_id='{RID}'),'target_runs',(SELECT count(*) FROM public.taxonomy_v2_import_runs WHERE release_id='{RID}'),'target_rows',(SELECT (SELECT count(*) FROM public.taxonomy_v2_taxa WHERE release_id='{RID}')+(SELECT count(*) FROM public.taxonomy_v2_vernacular_names WHERE release_id='{RID}')+(SELECT count(*) FROM public.taxonomy_v2_scientific_names WHERE release_id='{RID}')+(SELECT count(*) FROM public.taxonomy_v2_external_ids WHERE release_id='{RID}')+(SELECT count(*) FROM public.taxonomy_v2_redlist WHERE release_id='{RID}')),'migration',(SELECT count(*) FROM supabase_migrations.schema_migrations WHERE version='20261007123912'),'function',{FN_SQL},'timeout',current_setting('statement_timeout'),'validation',public.taxonomy_v2_validate_release('{PREV}'));")[0]
 fn=state.pop('function');report['state']=state
 b4=json.loads(B4.read_text())
 report['validator_function']=dict(fn_security(fn),source_matches_20261007123912=fn['source']==validator_expected(),security_equals_stage4b=fn_security(fn)==fn_security(b4['function']))
 print('state',json.dumps(state,ensure_ascii=False),flush=True)
 if state['active']!=[PREV]:stop('active release',state['active'])
 if state['target'] or state['target_runs'] or state['target_rows']:stop('partial target state',state)
 if state['concepts']!=52917 or state['prev_vernaculars']!=13760:stop('baseline counts',state)
 if state['migration']!=1 or not report['validator_function']['source_matches_20261007123912']:stop('validator definition')
 if not report['validator_function']['security_equals_stage4b']:stop('validator security changed',report['validator_function'])
 if state['timeout']!='2min' or not state['validation']['ok']:stop('timeout/validation',state)
 fp=fingerprint();report['fingerprint_before']=fp
 report['checks']['fingerprint_equals_stage4b_after']=fp==b4['invariants']
 if fp!=b4['invariants']:stop('production changed since Stage 4B',[k for k in fp if fp[k]!=b4['invariants'].get(k)])
 print('fingerprint unchanged since 4B',flush=True)
 s4=json.loads((S4/'preflight.json').read_text())['checks']
 for file,table in SPECS:
  observed=query(f"SELECT to_jsonb(t)-'release_id' FROM public.{table} t WHERE release_id='{PREV}';")
  expected=expected_rows(file,table,set(observed[0]))
  a={canon(x) for x in observed};b={canon(x) for x in expected}
  check={'before_count':len(observed),'expected_count':len(expected),'before_sha256':digest(observed),'candidate_sha256':digest(expected),'production_only':len(a-b),'candidate_only':len(b-a),'equals_stage4_baseline':digest(observed)==s4[table]['before_sha256']}
  report['checks'][table]=check
  print(table,json.dumps(check),flush=True)
  if a!=b or not check['equals_stage4_baseline']:stop('baseline mismatch '+table,check)
 for table in V3:
  rows=query(f'SELECT to_jsonb(t) FROM taxonomy_v3.{table} t;')
  check={'before_count':len(rows),'before_sha256':digest(rows),'equals_stage4_baseline':digest(rows)==s4['taxonomy_v3.'+table]['before_sha256']}
  report['checks']['taxonomy_v3.'+table]=check;print(table,json.dumps(check),flush=True)
  if not check['equals_stage4_baseline']:stop('taxonomy_v3 changed '+table,check)
 report['end_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();report['result']='READ_ONLY_PREFLIGHT_PASS'
 (S/'preflight.json').write_text(json.dumps(report,indent=2,ensure_ascii=False,default=str));print(report['result'],flush=True)
