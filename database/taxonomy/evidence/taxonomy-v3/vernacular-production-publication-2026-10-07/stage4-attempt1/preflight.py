import subprocess,json,hashlib,datetime
from pathlib import Path
S=Path.home()/'sporely-scratch/vernacular-2026-10-07-stage4'
F=Path.home()/'sporely-scratch/vernacular-2026-10-07-f1/run1/scoped'
REF='zkpjklzfwzefhjluvhfw';PREV='tax-2026.09.30-01'
def query(sql):
 p=subprocess.run(['docker','run','--rm','-i','--env-file',str(Path.home()/'.config/sporely/production-db.env'),'postgres:17','sh','-c','psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -Atf -'],input='BEGIN READ ONLY;\n'+sql+'\nCOMMIT;\n',text=True,capture_output=True)
 if p.returncode:raise RuntimeError(p.stderr)
 return [json.loads(x) for x in p.stdout.splitlines() if x]
def canon(x):return json.dumps(x,sort_keys=True,ensure_ascii=False,separators=(',',':'))
def digest(rows):return hashlib.sha256(('\n'.join(sorted(canon(x) for x in rows))+'\n').encode()).hexdigest()
report={'start_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'project_ref':REF,'previous_release':PREV,'checks':{},'writes_occurred':False}
state=query("SELECT jsonb_build_object('active',(SELECT jsonb_agg(release_id ORDER BY release_id) FROM public.taxonomy_v2_releases WHERE status='active'),'concepts',(SELECT count(*) FROM public.taxonomy_v2_concepts),'target',(SELECT count(*) FROM public.taxonomy_v2_releases WHERE release_id='tax-2026.10.07-01'),'runs',(SELECT count(*) FROM public.taxonomy_v2_import_runs WHERE release_id='tax-2026.10.07-01'),'validation',public.taxonomy_v2_validate_release('tax-2026.09.30-01'));")[0]
assert state['active']==[PREV] and state['target']==state['runs']==0 and state['validation']['ok'],state
report['state']=state
specs=[('taxon.jsonl','taxonomy_v2_taxa'),('scientific_name.jsonl','taxonomy_v2_scientific_names'),('taxon_external_id.jsonl','taxonomy_v2_external_ids'),('taxon_redlist.jsonl','taxonomy_v2_redlist')]
for file,table in specs:
 observed=query(f"SELECT to_jsonb(t)-'release_id' FROM public.{table} t WHERE release_id='{PREV}';")
 keys=set(observed[0]);expected=[]
 for line in (F/file).read_text().splitlines():
  x=json.loads(line);x['sporely_taxon_id']=x.pop('taxon_id')
  if table=='taxonomy_v2_taxa':x['parent_sporely_taxon_id']=x.pop('parent_taxon_id')
  if table=='taxonomy_v2_scientific_names':x['alias_reason']=x.pop('note')
  expected.append({k:x.get(k) for k in keys})
 a={canon(x) for x in observed};b={canon(x) for x in expected}
 check={'before_count':len(observed),'expected_count':len(expected),'before_sha256':digest(observed),'candidate_sha256':digest(expected),'production_only':len(a-b),'candidate_only':len(b-a)}
 report['checks'][table]=check
 (S/(table+'-baseline.json')).write_text(json.dumps(observed,ensure_ascii=False))
 print(table,json.dumps(check),flush=True)
 if a!=b:
  report['mismatch']={'table':table,'production_only_examples':sorted(a-b)[:3],'candidate_only_examples':sorted(b-a)[:3]}
  (S/'preflight.json').write_text(json.dumps(report,indent=2,ensure_ascii=False));raise RuntimeError('STOP_PRODUCTION_MISMATCH')
for table in ['registry_concept','external_mapping','identification_snapshot','resolution_link','release_installation','supplement_installation','reconciliation_manifest_audit']:
 rows=query(f'SELECT to_jsonb(t) FROM taxonomy_v3.{table} t;')
 report['checks']['taxonomy_v3.'+table]={'before_count':len(rows),'before_sha256':digest(rows)}
 (S/(table+'-baseline.json')).write_text(json.dumps(rows,ensure_ascii=False))
 print(table,len(rows),digest(rows),flush=True)
report['end_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();report['result']='READ_ONLY_PREFLIGHT_PASS'
(S/'preflight.json').write_text(json.dumps(report,indent=2,ensure_ascii=False))
print(report['result'],flush=True)
