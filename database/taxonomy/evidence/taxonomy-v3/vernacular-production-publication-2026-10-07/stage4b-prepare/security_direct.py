import subprocess
from pathlib import Path
s=Path.home()/'sporely-scratch/vernacular-2026-10-07-stage4b'
results=[]
for role in ['anon','authenticated']:
 for statement,expected in [("select * from public.search_taxa_v2('fixture','no',20);",0),("select * from public.resolve_taxon_external_id_v2('source','namespace','id');",0),("select * from public.taxonomy_v2_releases;",3),("select public.taxonomy_v2_activate_release('tax-2026.07.01-01');",3),("select public.taxonomy_v2_validate_release('tax-2026.07.01-01');",3)]:
  p=subprocess.run(['docker','exec','-i','supabase_db_zkpjklzfwzefhjluvhfw','psql','-X','-v','ON_ERROR_STOP=1','-U','postgres','-d','postgres','-f','-'],input='begin; set local role '+role+'; '+statement+' rollback;',text=True,capture_output=True)
  assert p.returncode==expected,(role,statement,p.stderr)
  if expected:assert 'permission denied' in p.stderr
  results.append((role,statement,p.returncode))
(s/'security-direct.out').write_text(str(results));print('10 direct-role checks PASS')
