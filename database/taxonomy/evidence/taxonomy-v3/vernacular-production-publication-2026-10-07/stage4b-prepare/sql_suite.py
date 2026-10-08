import subprocess
from pathlib import Path
r=Path('/Users/sigmundas/Documents/Code/sporely/sporely-web');s=Path.home()/'sporely-scratch/vernacular-2026-10-07-stage4b'
m=(r/'supabase/migrations/20261007123912_fix_taxonomy_v2_parent_reference_plan.sql').read_text()
for p in sorted((r/'supabase/tests').glob('taxonomy_v2_*_test.sql')):
 sql=p.read_text();assert '\nbegin;' in '\n'+sql.lower()
 sql=sql.replace('begin;','begin;\n'+m,1)
 x=subprocess.run(['docker','exec','-i','supabase_db_zkpjklzfwzefhjluvhfw','psql','-X','-v','ON_ERROR_STOP=1','-U','postgres','-d','postgres','-f','-'],input=sql,text=True,capture_output=True)
 (s/(p.stem+'.log')).write_text(x.stdout+'\n'+x.stderr);print(p.name,x.returncode,flush=True)
 if x.returncode:print(x.stderr[-1800:]);raise SystemExit(x.returncode)
