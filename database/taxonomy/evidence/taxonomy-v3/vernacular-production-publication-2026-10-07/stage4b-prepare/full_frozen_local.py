import subprocess,hashlib,json,time
from pathlib import Path
root=Path('/Users/sigmundas/Documents/Code/sporely/sporely-web');S=Path.home()/'sporely-scratch/vernacular-2026-10-07-stage4b'
p=Path.home()/'sporely-scratch/vernacular-2026-10-07-f1/run1/import.sql';h='cc9a1ddfc4c5f56fa553935b79fb40a2eda01588f0c6d2781e243cddda84852e'
assert hashlib.sha256(p.read_bytes()).hexdigest()==h
sql=p.read_text();assert sql.startswith('\\set ON_ERROR_STOP on\nBEGIN;') and sql.rstrip().endswith('COMMIT;')
migration=(root/'supabase/migrations/20261007123912_fix_taxonomy_v2_parent_reference_plan.sql').read_text()
# Local test stream only: install proposed function within original transaction,
# keep original load/validation/activation checks, and roll everything back.
sql=sql.replace('BEGIN;','BEGIN;\nSET LOCAL statement_timeout=\'2min\';\n'+migration,1)
sql=sql.rstrip()[:-len('COMMIT;')]+"EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT public.taxonomy_v2_validate_release('tax-2026.10.07-01');\nSELECT public.taxonomy_v2_validate_release('tax-2026.10.07-01');\nROLLBACK;\n"
q="SELECT count(*) FROM public.taxonomy_v2_taxa t WHERE t.release_id='tax-2026.10.07-01' AND t.parent_sporely_taxon_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_taxa p WHERE p.release_id=t.release_id AND p.sporely_taxon_id=t.parent_sporely_taxon_id%s)"
probes='EXPLAIN (VERBOSE,FORMAT JSON) '+q%''+';\nEXPLAIN (ANALYZE,BUFFERS,VERBOSE,FORMAT JSON) '+q%' OFFSET 0'+';\n'
sql=sql.replace('DO $validate$',probes+'DO $validate$',1)
start=time.monotonic()
with (S/'full-frozen-local.out').open('w') as out,(S/'full-frozen-local.err').open('w') as err:
 result=subprocess.run(['docker','exec','-i','supabase_db_zkpjklzfwzefhjluvhfw','psql','-X','-v','ON_ERROR_STOP=1','-U','postgres','-d','postgres','-f','-'],input=sql,text=True,stdout=out,stderr=err)
assert hashlib.sha256(p.read_bytes()).hexdigest()==h
report={'exit_code':result.returncode,'elapsed_seconds':time.monotonic()-start,'source_sql_unchanged':True,'timeout':'2min','target':'local Docker Supabase only','rollback':(S/'full-frozen-local.out').read_text().rstrip().endswith('ROLLBACK')}
(S/'full-frozen-local.json').write_text(json.dumps(report,indent=2));print(report)
if result.returncode:print((S/'full-frozen-local.err').read_text()[-2500:]);raise SystemExit(result.returncode)
