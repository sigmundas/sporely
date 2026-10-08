from pathlib import Path
S=Path.home()/'sporely-scratch/vernacular-2026-10-07-stage4'
exec((S/'preflight.py').read_text().split("report={'start_utc'")[0])
RID='tax-2026.10.07-01'
base=json.loads((S/'preflight.json').read_text())
report={'release_id':RID,'previous_release':PREV,'start_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'checks':{},'probes':[],'writes_occurred':True}
def save():
 (S/'production-verification.json').write_text(json.dumps(report,indent=2,ensure_ascii=False))
def require(condition,label,detail=None):
 if not condition:
  report['verdict']='STOP_PRODUCTION_MISMATCH';report['divergence']=label;report['detail']=detail;save();raise RuntimeError(label)
state=query(f"SELECT jsonb_build_object('releases',(SELECT jsonb_agg(jsonb_build_object('release_id',release_id,'status',status) ORDER BY release_id) FROM public.taxonomy_v2_releases),'active',(SELECT jsonb_agg(release_id) FROM public.taxonomy_v2_releases WHERE status='active'),'concepts',(SELECT count(*) FROM public.taxonomy_v2_concepts),'validation',public.taxonomy_v2_validate_release('{RID}'),'national_errors',public.taxonomy_v2_national_name_errors('{RID}'),'runs',(SELECT jsonb_agg(to_jsonb(r)) FROM public.taxonomy_v2_import_runs r WHERE release_id='{RID}'));")[0]
report['state']=state;save()
require(state['active']==[RID] and state['concepts']==52917 and state['validation']['ok'] and not state['national_errors'],'activation/count validation',state)
require(next(x['status'] for x in state['releases'] if x['release_id']==PREV)=='retired','previous release not retired')
require(len(state['runs'])==1 and state['runs'][0]['status']=='succeeded','import audit collision')
specs=[('taxon.jsonl','taxonomy_v2_taxa'),('scientific_name.jsonl','taxonomy_v2_scientific_names'),('taxon_external_id.jsonl','taxonomy_v2_external_ids'),('taxon_redlist.jsonl','taxonomy_v2_redlist'),('vernacular.jsonl','taxonomy_v2_vernacular_names')]
for file,table in specs:
 observed=query(f"SELECT to_jsonb(t)-'release_id' FROM public.{table} t WHERE release_id='{RID}';")
 keys=set(observed[0]);expected=[]
 for line in (F/file).read_text().splitlines():
  x=json.loads(line);x['sporely_taxon_id']=x.pop('taxon_id')
  if table=='taxonomy_v2_taxa':x['parent_sporely_taxon_id']=x.pop('parent_taxon_id')
  if table=='taxonomy_v2_scientific_names':x['alias_reason']=x.pop('note')
  expected.append({k:x.get(k) for k in keys})
 a={canon(x) for x in observed};b={canon(x) for x in expected}
 check={'after_count':len(observed),'expected_count':len(expected),'after_sha256':digest(observed),'candidate_sha256':digest(expected),'unexpected_rows':len(a-b),'missing_rows':len(b-a),'duplicates':len(observed)-len(a)}
 if table in base['checks']:check['unchanged_from_baseline']=digest(observed)==base['checks'][table]['before_sha256']
 report['checks'][table]=check;save();print(table,check,flush=True)
 require(a==b and len(observed)==len(expected) and len(a)==len(observed),'frozen content mismatch '+table,check)
 if table!='taxonomy_v2_vernacular_names':require(check['unchanged_from_baseline'],'invariant changed '+table)
 else:
  vernacular=observed
  markers=[x for x in observed if __import__('re').search(r'\[\s*(GAMMELT|UTGÅTT)\s*\]',x['vernacular_name'],__import__('re').I)]
  require(not markers,'obsolete vernacular marker',markers)
  report['nordic_isolation_rows']=[x for x in observed if x['sporely_taxon_id'] in [96276,19080,58651,58832,58815,58814,58722] and x['language_code'] in ['nb','nn','no']]
for table in ['registry_concept','external_mapping','identification_snapshot','resolution_link','release_installation','supplement_installation','reconciliation_manifest_audit']:
 rows=query(f'SELECT to_jsonb(t) FROM taxonomy_v3.{table} t;')
 expected=base['checks']['taxonomy_v3.'+table]
 check={'after_count':len(rows),'after_sha256':digest(rows),'unchanged':len(rows)==expected['before_count'] and digest(rows)==expected['before_sha256']}
 report['checks']['taxonomy_v3.'+table]=check;save();print(table,check['unchanged'],flush=True)
 require(check['unchanged'],'protected taxonomy_v3 changed '+table,check)
probes=json.loads(Path('database/taxonomy/evidence/taxonomy-v3/vernacular-f1-refreeze-2026-10-07/search-probes.json').read_text())['probes']
lit=lambda s:"'"+s.replace("'","''")+"'"
sql='SET LOCAL ROLE authenticated;\n'
for p in probes:
 sql+=f"SELECT jsonb_build_object('query',{lit(p['query'])},'results',(SELECT coalesce(jsonb_agg(to_jsonb(s) ORDER BY s.ordinality),'[]'::jsonb) FROM public.search_taxa_v2({lit(p['query'])},'no',50) WITH ORDINALITY s));\n"
answers=query(sql)
for p,ans in zip(probes,answers):
 results=ans['results'];target=p.get('expected_target');check={'query':p['query'],'expected_target':target,'results':results}
 if target:
  reference=next(r for r in p['results'] if r['taxon_id']==target)
  check['pass']=bool(results) and results[0]['taxon_id']==target and results[0]['scientific_name']==reference['scientific_name'] and results[0]['matched_name']==reference['matched_name']
 else:check['pass']=[(r['taxon_id'],r['scientific_name'],r['matched_name']) for r in results]==[(r['taxon_id'],r['scientific_name'],r['matched_name']) for r in p['results']]
 report['probes'].append(check);save();print('probe',p['query'],check['pass'],flush=True)
 require(check['pass'],'production search differs '+p['query'],check)
report['end_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();report['verdict']='PRODUCTION_RELEASE_VERIFIED';save();print(report['verdict'],flush=True)
