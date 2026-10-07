import json,hashlib,subprocess,datetime
from pathlib import Path
S=Path.home()/'sporely-scratch/vernacular-2026-10-07-stage4c'
payload=Path.home()/'sporely-scratch/vernacular-2026-10-07-f1/run1/import.sql'
expected='cc9a1ddfc4c5f56fa553935b79fb40a2eda01588f0c6d2781e243cddda84852e'
assert hashlib.sha256(payload.read_bytes()).hexdigest()==expected
pre=json.loads((S/'preflight.json').read_text());assert pre['result']=='READ_ONLY_PREFLIGHT_PASS'
cmd=['docker','run','--rm','--env-file',str(Path.home()/'.config/sporely/production-db.env'),'-v',str(payload.parent)+':/payload:ro','postgres:17','sh','-c','psql "$DATABASE_URL" -X --file=/payload/import.sql']
report={'release_id':'tax-2026.10.07-01','project_ref':'zkpjklzfwzefhjluvhfw','freeze_sha256':'a86e35854fd4d01984d8b8fe121d8fc318e75a01876d243975847127462fd14e','artifact_sha256':expected,'artifact_bytes':payload.stat().st_size,'artifact_path':str(payload),'command':cmd,'start_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
(S/'import-execution.json').write_text(json.dumps(report,indent=2))
print('Import start',report['start_utc'],expected,flush=True)
with (S/'import.stdout.log').open('w') as out,(S/'import.stderr.log').open('w') as err:
 result=subprocess.run(cmd,stdout=out,stderr=err)
report['end_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();report['exit_code']=result.returncode
report['artifact_unchanged']=hashlib.sha256(payload.read_bytes()).hexdigest()==expected
report['commit_reported']=(S/'import.stdout.log').read_text().rstrip().endswith('COMMIT')
(S/'import-execution.json').write_text(json.dumps(report,indent=2))
print('Import end',report['end_utc'],'exit',result.returncode,'commit',report['commit_reported'],flush=True)
if result.returncode:print((S/'import.stderr.log').read_text()[-2500:]);raise SystemExit(result.returncode)
