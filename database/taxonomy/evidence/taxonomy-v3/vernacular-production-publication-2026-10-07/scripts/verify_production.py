import json,re,datetime,subprocess
from pathlib import Path
from preflight import *
PROBES=Path('/Users/sigmundas/Documents/Code/sporely/sporely-py/database/taxonomy/evidence/taxonomy-v3/vernacular-f1-refreeze-2026-10-07/search-probes.json')
base=json.loads((S/'preflight.json').read_text());assert base['result']=='READ_ONLY_PREFLIGHT_PASS'
report={'release_id':RID,'previous_release':PREV,'freeze_sha256':FREEZE,'import_sql_sha256':SQLH,'start_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'checks':{},'probes':[],'findings':[],'writes_occurred':True}
def save():(S/'production-verification.json').write_text(json.dumps(report,indent=2,ensure_ascii=False,default=str))
def require(cond,label,detail=None):
 if not cond:
  report['verdict']='STOP_PRODUCTION_MISMATCH';report['divergence']=label;report['detail']=detail;save();raise SystemExit('STOP: '+label+' '+json.dumps(detail,ensure_ascii=False,default=str)[:2000])
# D/C. release state + validator
state=query(f"SELECT jsonb_build_object('releases',(SELECT jsonb_agg(jsonb_build_object('release_id',release_id,'status',status) ORDER BY release_id) FROM public.taxonomy_v2_releases),'active',(SELECT jsonb_agg(release_id) FROM public.taxonomy_v2_releases WHERE status='active'),'concepts',(SELECT count(*) FROM public.taxonomy_v2_concepts),'new_vernaculars',(SELECT count(*) FROM public.taxonomy_v2_vernacular_names WHERE release_id='{RID}'),'prev_vernaculars',(SELECT count(*) FROM public.taxonomy_v2_vernacular_names WHERE release_id='{PREV}'),'dangling_parents',(SELECT count(*) FROM public.taxonomy_v2_taxa t WHERE t.release_id='{RID}' AND t.parent_sporely_taxon_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_taxa p WHERE p.release_id=t.release_id AND p.sporely_taxon_id=t.parent_sporely_taxon_id OFFSET 0)),'validation',public.taxonomy_v2_validate_release('{RID}'),'national_errors',public.taxonomy_v2_national_name_errors('{RID}'),'runs',(SELECT jsonb_agg(to_jsonb(r)) FROM public.taxonomy_v2_import_runs r WHERE release_id='{RID}'),'timeout',current_setting('statement_timeout'),'function',{FN_SQL});")[0]
fn=state.pop('function');state['validator_source_matches']=fn['source']==validator_expected();state['validator_security_unchanged']=fn_security(fn)=={k:base['validator_function'][k] for k in ['owner','acl','settings','security_definer','volatility']}
report['state']=state;save();print('state',json.dumps({k:v for k,v in state.items() if k not in ['runs','validation']},ensure_ascii=False),flush=True)
require(state['active']==[RID],'active release',state['active'])
require(next(x['status'] for x in state['releases'] if x['release_id']==PREV)=='retired','previous release not retired',state['releases'])
require(state['concepts']==52917 and state['new_vernaculars']==59884 and state['prev_vernaculars']==13760,'counts',state)
# Accepted scoped-root condition (owner decision 2026-10-07): exactly one declared dangling parent, Fungi 152331 -> 150361 outside scope.
dang=query(f"SELECT jsonb_build_object('declared',(SELECT dangling_parent_count FROM public.taxonomy_v2_releases WHERE release_id='{RID}'),'rows',(SELECT coalesce(jsonb_agg(jsonb_build_object('sporely_taxon_id',t.sporely_taxon_id,'canonical_scientific_name',t.canonical_scientific_name,'parent_sporely_taxon_id',t.parent_sporely_taxon_id)),'[]'::jsonb) FROM public.taxonomy_v2_taxa t WHERE t.release_id='{RID}' AND t.parent_sporely_taxon_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_taxa p WHERE p.release_id=t.release_id AND p.sporely_taxon_id=t.parent_sporely_taxon_id OFFSET 0)),'parent_in_release',(SELECT count(*) FROM public.taxonomy_v2_taxa WHERE release_id='{RID}' AND sporely_taxon_id=150361));")[0]
dang['parent_in_frozen_scope']=any(json.loads(l)['taxon_id']==150361 for l in (F/'taxon.jsonl').read_text().splitlines())
state['dangling']=dang;save()
require(state['dangling_parents']==dang['declared']==len(dang['rows'])==1,'dangling count vs declared metadata',dang)
require(dang['rows']==[{'sporely_taxon_id':152331,'canonical_scientific_name':'Fungi','parent_sporely_taxon_id':150361}],'unexpected dangling row',dang)
require(dang['parent_in_release']==0 and not dang['parent_in_frozen_scope'],'parent 150361 inside release scope',dang)
report['findings'].append('accepted scoped-root condition: declared dangling_parent_count=1, sole row Fungi 152331 -> parent 150361 (outside release scope); identical in all prior releases')
require(state['validation']['ok'] and not state['national_errors'],'validator',state['validation'])
require(len(state['runs'])==1 and state['runs'][0]['status']=='succeeded','import run',state['runs'])
require(state['timeout']=='2min' and state['validator_source_matches'] and state['validator_security_unchanged'],'validator definition/timeout')
# validator timing
p=subprocess.run(['docker','run','--rm','-i','--env-file',str(ENV),'postgres:17','sh','-c','psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -Atf -'],input=f"BEGIN READ ONLY;\nEXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT public.taxonomy_v2_validate_release('{RID}');\nEXPLAIN (ANALYZE,FORMAT JSON) SELECT count(*) FROM public.taxonomy_v2_taxa t WHERE t.release_id='{RID}' AND t.parent_sporely_taxon_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_taxa p WHERE p.release_id=t.release_id AND p.sporely_taxon_id=t.parent_sporely_taxon_id OFFSET 0);\nROLLBACK;\n",text=True,capture_output=True)
require(p.returncode==0,'validator timing',p.stderr)
dec=json.JSONDecoder();txt=p.stdout.strip();a,i=dec.raw_decode(txt);b,_=dec.raw_decode(txt[i:].strip())
report['validator_timing_ms']={'full_validator':a[0]['Execution Time'],'parent_check':b[0]['Execution Time']};save();print('timing',report['validator_timing_ms'],flush=True)
# F. fingerprint: pre-existing rows unchanged; only allowed tables grew
fb=base['fingerprint_before']
parts=["'%s',(SELECT jsonb_build_object('count',count(*),'hash',md5(coalesce(string_agg(to_jsonb(t)::text,E'\\n' ORDER BY to_jsonb(t)::text),''))) FROM %s t WHERE %s)"%(t,t,("t.release_id IS DISTINCT FROM '%s'"%RID) if t.startswith('public.taxonomy_v2_') and t not in ('public.taxonomy_v2_concepts','public.taxonomy_v2_releases','public.taxonomy_v2_import_runs') else 'true') for t in FP_TABLES]
fa=query("SELECT jsonb_build_object("+','.join(parts)+");")[0];report['fingerprint_after_excluding_target']=fa
changed=[t for t in FP_TABLES if fa[t]!=fb[t]];report['checks']['fingerprint_changed_tables']=changed;save();print('changed tables',changed,flush=True)
require(set(changed)<={'public.taxonomy_v2_releases','public.taxonomy_v2_import_runs'},'pre-existing rows changed',changed)
require(fa['public.taxonomy_v2_releases']['count']==fb['public.taxonomy_v2_releases']['count']+1 and fa['public.taxonomy_v2_import_runs']['count']==fb['public.taxonomy_v2_import_runs']['count']+1,'release/run counts')
old=query(f"SELECT jsonb_build_object('releases',(SELECT jsonb_agg(to_jsonb(r)-'status'-'retired_at'-'updated_at'-'activated_at' ORDER BY release_id) FROM public.taxonomy_v2_releases r WHERE release_id<>'{RID}'),'runs',md5(coalesce((SELECT string_agg(to_jsonb(r)::text,E'\\n' ORDER BY to_jsonb(r)::text) FROM public.taxonomy_v2_import_runs r WHERE release_id<>'{RID}'),'')),'prev_row',(SELECT to_jsonb(r) FROM public.taxonomy_v2_releases r WHERE release_id='{PREV}'));")[0]
report['checks']['old_import_runs_md5']=old['runs'];report['checks']['prev_release_row']=old['prev_row'];save()
# new-release content equals frozen candidate; non-vernacular equals prior baseline
for file,table in SPECS+[('vernacular.jsonl','taxonomy_v2_vernacular_names')]:
 observed=query(f"SELECT to_jsonb(t)-'release_id' FROM public.{table} t WHERE release_id='{RID}';")
 expected=expected_rows(file,table,set(observed[0]))
 a={canon(x) for x in observed};b={canon(x) for x in expected}
 check={'after_count':len(observed),'expected_count':len(expected),'after_sha256':digest(observed),'candidate_sha256':digest(expected),'unexpected_rows':len(a-b),'missing_rows':len(b-a),'duplicates':len(observed)-len(a)}
 if table in base['checks']:check['unchanged_from_baseline']=digest(observed)==base['checks'][table]['before_sha256']
 report['checks'][table]=check;save();print(table,json.dumps(check),flush=True)
 require(a==b and len(observed)==len(expected) and len(a)==len(observed),'frozen content mismatch '+table,check)
 if table!='taxonomy_v2_vernacular_names':require(check['unchanged_from_baseline'],'invariant changed '+table,check)
 else:vern=observed
# Phase E isolation / markers / spelling
markers=[x for x in vern if re.search(r'\[\s*(GAMMELT|UTGÅTT)\s*\]',x['vernacular_name'],re.I)];report['checks']['obsolete_markers']=len(markers)
require(not markers,'obsolete vernacular marker',markers)
NORD={'nb','nn','no'}
nord=lambda tid:sorted((x['language_code'],x['vernacular_name']) for x in vern if x['sporely_taxon_id']==tid and x['language_code'] in NORD)
iso={tid:nord(tid) for tid in [96276,19080,58651,58832,58815,58814]};report['checks']['isolation']={str(k):v for k,v in iso.items()};save()
require(iso[96276]==[('nb','silkerødspore'),('nn','silkeraudspore')],'96276 isolation',iso[96276])
require(not any('gruppe' in n for _,n in iso[96276]),'silkerødspore-gruppen attached to 96276')
for tid in [19080,58651,58832,58815]:require(iso[tid]==[],f'{tid} isolation',iso[tid])
require(iso[58814]==[('nb','taigarødtuppsopp')],'58814 isolation',iso[58814])
names={(x['sporely_taxon_id'],x['language_code'],x['vernacular_name']) for x in vern}
sp=json.loads((FR/'run1/spelling-replacements.json').read_text());miss=[];hist=[]
for r in sp:
 if r['scoped_names'] is None:continue
 for c in r['current']:
  if (r['target'],r['language'],c) not in names:miss.append(r)
 if r['historical'] not in r['current'] and (r['target'],r['language'],r['historical']) in names:hist.append(r)
report['checks']['spelling_replacements']={'checked':sum(r['scoped_names'] is not None for r in sp),'current_missing':len(miss),'historical_present':len(hist)};save()
print('spelling',report['checks']['spelling_replacements'],flush=True)
require(not miss and not hist,'spelling replacements',{'missing':miss[:5],'historical':hist[:5]})
# search probes
probes=json.loads(PROBES.read_text())['probes'];lit=lambda s:"'"+s.replace("'","''")+"'"
sql='SET LOCAL ROLE authenticated;\n'+''.join(f"SELECT jsonb_build_object('query',{lit(p['query'])},'results',(SELECT coalesce(jsonb_agg(to_jsonb(s) ORDER BY s.ordinality),'[]'::jsonb) FROM public.search_taxa_v2({lit(p['query'])},'no',50) WITH ORDINALITY s));\n" for p in probes)
answers=query(sql)
for p,ans in zip(probes,answers):
 res=[dict(r,scientific_name=r['display_scientific_name']) for r in ans['results']];target=p.get('expected_target');check={'query':p['query'],'expected_target':target,'top':[(r['taxon_id'],r['scientific_name'],r['matched_name']) for r in res[:3]]}
 if target:
  ref=next(r for r in p['results'] if r['taxon_id']==target)
  check['pass']=bool(res) and res[0]['taxon_id']==target and res[0]['scientific_name']==ref['scientific_name'] and res[0]['matched_name']==ref['matched_name']
  check['equals_stage3_full']=[(r['taxon_id'],r['matched_name']) for r in res]==[(r['taxon_id'],r['matched_name']) for r in p['results']]
 else:check['pass']=[(r['taxon_id'],r['scientific_name'],r['matched_name']) for r in res]==[(r['taxon_id'],r['scientific_name'],r['matched_name']) for r in p['results']]
 report['probes'].append(check);save();print('probe',p['query'],check['pass'],check.get('equals_stage3_full',''),check['top'][:1],flush=True)
 require(check['pass'],'production search differs '+p['query'],check)
 if target and not check['equals_stage3_full']:report['findings'].append('ranking tail differs from Stage 3 for '+p['query'])
report['end_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
report['verdict']='PRODUCTION_RELEASE_VERIFIED_WITH_NONBLOCKING_FINDINGS' if report['findings'] else 'PRODUCTION_RELEASE_VERIFIED';save();print(report['verdict'],flush=True)
