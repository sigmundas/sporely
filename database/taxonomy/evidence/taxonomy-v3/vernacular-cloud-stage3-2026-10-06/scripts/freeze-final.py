# Reconstruction of /tmp/sporely-vernacular-stage2/{validation receipt step, freeze-final.py} (Codex session 2026-10-04T19:51-19:53Z).
# Only input locations differ (re-materialized in durable scratch); member names, receipt content and freeze() call are identical.
import sys,json,hashlib
from pathlib import Path
sys.path.insert(0,str(Path('database/taxonomy/scripts').resolve()))
from promote_desktop_bundle import freeze,verify_frozen
S=Path.home()/'sporely-scratch/vernacular-2026-10-06'
B=S/'buildA';D=S/'diag-headdyntaxa'
root=S/'stage2';evidence=Path('database/taxonomy/evidence/taxonomy-v3/vernacular-compiler-stage1b-2026-10-04')
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
v=json.loads((evidence/'verification.json').read_text())
assert sha(D/'frozenA.sqlite3')==v['sqlite_sha256']
for n,h in v['compiler_artifact_sha256'].items():assert sha(D/'release'/n)==h
receipt={'validated':True,'sqlite_sha256':v['sqlite_sha256'],'compiler_manifest_sha256':sha(D/'release/manifest.json'),'expected_vernacular_enrichment':{'automatic':1619,'reviewed':17,'total':1636,'affected_concepts':766},'stage1b_verification':v}
(root/'validation-receipt.json').write_text(json.dumps(receipt,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
inputs={'release-recipe.json':Path('database/taxonomy/release-recipe.json'),
 'owner-reviews.json':Path('database/taxonomy/policies/vernacular_associations.json'),
 'mapping-policy.json':Path('database/taxonomy/policies/mapping_policy.yml'),
 'manual-mappings.json':Path('database/taxonomy/policies/manual_mappings.yml'),
 'supersessions.json':Path('database/taxonomy/policies/concept_supersessions.yml'),
 'scope-policy.json':Path('database/taxonomy/policies/global-macrofungi-scope.yml'),
 'col-normalization.json':B/'norm/col_xr/report.json',
 'col-vernacular-rejections.jsonl':B/'norm/col_xr/vernacular_rejections.jsonl',
 'nortaxa-normalization.json':B/'norm/nortaxa/report.json',
 'dyntaxa-normalization.json':D/'dyntaxa/report.json'}
for source,version in [('col_xr','2026-07-17-XR'),('nortaxa','1.284'),('dyntaxa','2026-09-30')]:inputs[source+'-acquisition.json']=Path(f'database/taxonomy/sources/{source}/{version}/manifest.json')
for p in evidence.iterdir():
 if p.is_file():inputs['stage1b-'+p.name]=p
for p in (evidence/'removal-audit').iterdir():
 if p.is_file():inputs['stage1b-removal-'+p.name]=p
for run in sys.argv[1:] or ['A','B']:
 print('Freezing',run,flush=True)
 freeze(sqlite_path=D/'frozenA.sqlite3',release_dir=D/'release',redlist_report_path=B/'norm/redlist_no/report.json',redlist_workbook=Path('database/taxonomy/national_sources/artsdatabanken_redlist/2021/redlist-2021.xlsx'),output_dir=root/('final'+run),validation_receipt_path=root/'validation-receipt.json',evidence_inputs=inputs)
 verify_frozen(root/('final'+run))
 print('Verified',run,flush=True)
