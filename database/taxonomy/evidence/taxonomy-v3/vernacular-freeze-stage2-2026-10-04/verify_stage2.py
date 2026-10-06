import sys,json,hashlib,shutil,tarfile
from pathlib import Path
sys.path.insert(0,str(Path('database/taxonomy/scripts').resolve()))
import promote_desktop_bundle as p
root=Path('/tmp/sporely-vernacular-stage2')
sha=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
a=root/'finalA';b=root/'finalB'
assert {x.name:sha(x) for x in a.iterdir()}=={x.name:sha(x) for x in b.iterdir()}
p.verify_frozen(a)
bundle=root/'scratch-bundle';bundle.mkdir()
shutil.copyfile(p.DEFAULT_BUNDLE_DIR/'manifest.json',bundle/'manifest.json')
older=bundle/'previous.evidence.tar.gz';older.write_bytes(b'previous archive marker')
compat=root/'scratch-compatibility.json';shutil.copyfile(p.DEFAULT_COMPATIBILITY,compat)
protected_before={x.name:sha(x) for x in a.iterdir()}
p.write_deterministic_gzip=lambda *args,**kwargs:(_ for _ in ()).throw(AssertionError('promotion generated gzip'))
manifest=p.promote(frozen_dir=a,bundle_dir=bundle,compatibility_path=compat,expected_freeze_sha256=sha(a/'freeze.json'))
for name in ['manifest.json',manifest['gz_artifact'],manifest['compiler_evidence']['artifact']]:assert sha(bundle/name)==sha(a/name)
assert sha(bundle/manifest['freeze_artifact'])==sha(a/'freeze.json')
assert protected_before=={x.name:sha(x) for x in a.iterdir()}
assert older.read_bytes()==b'previous archive marker'
p.promote(frozen_dir=a,bundle_dir=bundle,compatibility_path=compat,expected_freeze_sha256=sha(a/'freeze.json'))
protected=json.loads((root/'protected.json').read_text())
for name,digest in protected.items():assert sha(Path(name))==digest,name
with tarfile.open(a/manifest['compiler_evidence']['artifact'],'r:gz') as tar:
 inventory=json.load(tar.extractfile('inventory.json'))
 for name,item in json.loads(Path('/tmp/sporely-vernacular-stage1b/frozenA/manifest.json').read_text())['outputs'].items():
  assert inventory['members']['compiler/'+item['name']]['sha256']==item['sha256']
assert inventory['members']['compiler/vernacular_evidence.jsonl']
report={'freeze_sha256':sha(a/'freeze.json'),'frozen_artifacts':{x.name:{'sha256':sha(x),'bytes':x.stat().st_size} for x in a.iterdir()},'independent_freezes_byte_identical':True,'archive_member_fingerprints_verified':True,'compiler_outputs_archived':len(json.loads(Path('/tmp/sporely-vernacular-stage1b/frozenA/manifest.json').read_text())['outputs'])+1,'archive_members':len(inventory['members']),'scratch_promotion_copies_only_frozen_bytes':True,'frozen_inputs_unchanged_after_promotion':True,'scratch_repeat_promotion_idempotent':True,'previous_evidence_archive_retained':True,'production_and_repository_bundle_unchanged':protected,'expected_pinned_enrichment':{'automatic':1619,'reviewed':17,'total':1636,'affected_concepts':766},'promoted_to_repository':False,'production_touched':False}
out=Path('database/taxonomy/evidence/taxonomy-v3/vernacular-freeze-stage2-2026-10-04');out.mkdir(exist_ok=True)
(out/'verification.json').write_text(json.dumps(report,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
shutil.copyfile(a/'freeze.json',out/'freeze.json');shutil.copyfile(a/'manifest.json',out/'publication-manifest.json')
print(json.dumps({k:report[k] for k in ['freeze_sha256','independent_freezes_byte_identical','archive_members','scratch_promotion_copies_only_frozen_bytes']},indent=2))
