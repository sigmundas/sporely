import json,hashlib,sqlite3,gzip,collections,sys
from pathlib import Path
sys.path.insert(0,str(Path('database/taxonomy/scripts').resolve()))
import vernacular_projection as v
base=Path('/tmp/sporely-vernacular-stage1b');oldbase=Path('/tmp/sporely-vernacular-stage1');out=Path('database/taxonomy/evidence/taxonomy-v3/vernacular-compiler-stage1b-2026-10-04');out.mkdir(exist_ok=True)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def lines(p):return [json.loads(l) for l in p.open()]
def names(db):return {(r[0],r[1],r[2]):bool(r[3]) for r in db.execute('select taxon_id,language_code,vernacular_name,is_preferred_name from vernacular_min')}
def norm(s):return v.norm(s)
a=base/'frozenA';b=base/'frozenB';files={p.name:sha(p) for p in a.iterdir() if p.is_file()}
assert files=={p.name:sha(p) for p in b.iterdir() if p.is_file()}
assert sha(base/'frozenA.sqlite3')==sha(base/'frozenB.sqlite3')
identity={}
for n in ['taxa.jsonl','source_usages.jsonl','mappings.jsonl','legacy_external_ids.jsonl']:
 assert sha(a/n)==sha(oldbase/'frozenA'/n),n
 identity[n]=sha(a/n)
assert sha(base/'registry-frozenA.jsonl')==sha(base/'registry-frozenB.jsonl')==sha(oldbase/'registry-base.jsonl')
old=sqlite3.connect('file:/tmp/sporely-no-coverage-bundle.sqlite3?mode=ro',uri=True)
prev=sqlite3.connect(f'file:{oldbase}/frozenA.sqlite3?mode=ro',uri=True)
new=sqlite3.connect(f'file:{base}/frozenA.sqlite3?mode=ro',uri=True)
for table in ['taxon_external_id_min','taxon_external_id_text_min','taxon_min','scientific_name_min']:
 assert list(prev.execute(f'select * from {table} order by 1'))==list(new.execute(f'select * from {table} order by 1')),table
assert new.execute('pragma integrity_check').fetchone()[0]=='ok'
assert not list(new.execute('pragma foreign_key_check'))
on,pn,nn=names(old),names(prev),names(new)
assert pn.keys() <= nn.keys()
normalized_new={(tid,lang,norm(name)) for tid,lang,name in nn}
old_audit=list(map(json.loads,gzip.open('database/taxonomy/evidence/taxonomy-v3/vernacular-removal-audit-2026-10-04/removed-row-classification.jsonl.gz','rt')))
gaps=[r for r in old_audit if r['classification']=='current_evidence_exists_but_compiler_does_not_recover_it']
assert len(gaps)==10331 and len({r['target_sporely_taxon_id'] for r in gaps})==3579
compiled=lines(a/'vernacular.jsonl');col_entries=[r for r in compiled if r['source_code']=='col_xr']
col_keys={(r['sporely_taxon_id'],r['language'],norm(r['vernacular_name'])) for r in col_entries}
col_index=collections.defaultdict(list)
for x in col_entries:col_index[(x["sporely_taxon_id"],x["language"],norm(x["vernacular_name"]))].append(x)
recoveries=[]
for r in gaps:
 key=(r['target_sporely_taxon_id'],r['language'],norm(r['vernacular_name']))
 assert key in normalized_new and key in col_keys,key
 records=col_index[key]
 assert all(x['core_row_id']['value']==r['target_canonical_usage_id'] for x in records)
 recoveries.append({'historical_row':[r['target_sporely_taxon_id'],r['language'],r['vernacular_name']],
  'current_col_usage_id':r['target_canonical_usage_id'],'current_names':[x['vernacular_name'] for x in records],
  'current_source_provenance':[x['provenance'] for x in records]})
unsupported=[r for r in old_audit if not r['current_pinned_target_association_exists']]
assert len(unsupported)==8453
assert not any((r['target_sporely_taxon_id'],r['language'],norm(r['vernacular_name'])) in normalized_new for r in unsupported)
normalized_col=lines(base/'col/vernacular.jsonl')
canonical={r['canonical_source_usage']['identifier']:r['sporely_taxon_id'] for r in lines(a/'taxa.jsonl') if r['canonical_source_code']=='col_xr'}
assert all((canonical[r['core_row_id']['value']],r['language'],r['vernacular_name']) in nn for r in normalized_col if r['core_row_id']['value'] in canonical)
assert len(col_entries)==len(normalized_col)==89926
# Independently derivable Norwegian association evidence remains unchanged.
e=lines(a/'vernacular_evidence.jsonl');old_e=lines(oldbase/'frozenA/vernacular_evidence.jsonl')
added=[r for r in e if r.get('projection_status')=='added'];prior_added=[r for r in old_e if r.get('projection_status')=='added']
assert added==prior_added
assert len([r for r in added if r['evidence_class']==v.AUTOMATIC])==1619
assert len([r for r in added if r['evidence_class']==v.REVIEWED])==17
assert all(len(r['full_bundle_target_ids'])==1 for r in added if r['evidence_class']==v.AUTOMATIC)
for k in [(61673,'nb','grønnkremle'),(96276,'nb','silkerødspore'),(625268,'nb','silkerødspore-gruppen')]:assert k in nn
assert (96276,'nb','silkerødspore-gruppen') not in nn
# Existing national names, including grønn navlesopp, retain preferred status.
assert all(nn[k]==pn[k] for k in pn)
preferred_old=[{'row':k,'before':on[k],'after':nn[k]} for k in sorted(on.keys()&nn.keys()) if on[k]!=nn[k]]
preferred_stage1=[{'row':k,'before':pn[k],'after':nn[k]} for k in sorted(pn.keys()&nn.keys()) if pn[k]!=nn[k]]
assert preferred_stage1==[]
for k in pn:
 if 'grønn navlesopp'==k[2]:assert new.execute('select source,is_preferred_name from vernacular_min where taxon_id=? and language_code=? and vernacular_name=?',k).fetchone()==prev.execute('select source,is_preferred_name from vernacular_min where taxon_id=? and language_code=? and vernacular_name=?',k).fetchone()
validations=json.loads((a/'vernacular_reviews.json').read_text())['validations'];assert len(validations)==10 and all(r['status']=='reused' for r in validations)
protected=json.loads((base/'protected.json').read_text())
for p,h in protected.items():assert sha(Path(p))==h,p
report={'recovered_audited_rows':10331,'recovered_audited_concepts':3579,'unsupported_associations_still_withheld':8453,'normalized_col_rows':89926,'col_archive_normalization':json.loads((base/'col/report.json').read_text())['vernacular'],'old_bundle_rows':len(on),'stage1_rows':len(pn),'stage1b_rows':len(nn),'additions_vs_old':len(nn.keys()-on.keys()),'additions_vs_stage1':len(nn.keys()-pn.keys()),'exact_retained_vs_old':len(nn.keys()&on.keys()),'exact_removed_vs_old':len(on.keys()-nn.keys()),'removed_vs_stage1':len(pn.keys()-nn.keys()),'removals_by_language':dict(collections.Counter(k[1] for k in on.keys()-nn.keys())),'preferred_changes_vs_old':preferred_old,'preferred_changes_vs_stage1':preferred_stage1,'norwegian_automatic':1619,'norwegian_reviewed':17,'norwegian_association_evidence_byte_equivalent':True,'identity_artifacts_byte_identical':identity,'sqlite_identity_tables_identical':True,'registry_byte_identical':True,'compiler_repeat_byte_identical':True,'sqlite_repeat_byte_identical':True,'compiler_artifact_sha256':files,'sqlite_sha256':sha(base/'frozenA.sqlite3'),'protected_inputs_sha256':protected,'no_current_canonical_col_name_silently_lost':True,'production_touched':False,'promoted':False}
(out/'verification.json').write_text(json.dumps(report,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
(out/'recovered-current-col-associations.json').write_text(json.dumps(recoveries,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
(out/'preferred-name-delta.json').write_text(json.dumps({'vs_old_bundle':preferred_old,'vs_stage1':preferred_stage1},ensure_ascii=False,sort_keys=True,indent=2)+'\n')
print(json.dumps({k:report[k] for k in ['recovered_audited_rows','stage1b_rows','additions_vs_old','additions_vs_stage1','exact_retained_vs_old','exact_removed_vs_old','removed_vs_stage1','removals_by_language']},indent=2))
