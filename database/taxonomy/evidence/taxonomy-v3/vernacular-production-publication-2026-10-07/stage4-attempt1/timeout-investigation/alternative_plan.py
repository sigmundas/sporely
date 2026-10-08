import sys
from pathlib import Path
sys.path.insert(0,str(Path.home()/'sporely-scratch/vernacular-2026-10-07-stage4'))
from investigate_readonly import run
q="SELECT count(*) FROM public.taxonomy_v2_taxa t WHERE t.release_id='%s' AND t.parent_sporely_taxon_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_taxa p WHERE p.release_id=t.release_id AND p.sporely_taxon_id=t.parent_sporely_taxon_id OFFSET 0)"
run('candidate_nonflattened_plan','EXPLAIN (VERBOSE, FORMAT JSON) '+q%'tax-2026.10.07-01'+';')
run('active_nonflattened_measured','EXPLAIN (ANALYZE,BUFFERS,VERBOSE,FORMAT JSON) '+q%'tax-2026.09.30-01'+';')
