import sys,json,hashlib,collections
from pathlib import Path
sys.path.insert(0,str(Path.home()/'sporely-scratch/vernacular-2026-10-07-stage4'))
from investigate_readonly import run
run('timeout_scope_proof',"SELECT jsonb_build_object('phase','before','timeout',current_setting('statement_timeout'),'source',(SELECT source FROM pg_settings WHERE name='statement_timeout')); SET LOCAL statement_timeout='121s'; SELECT jsonb_build_object('phase','local_probe','timeout',current_setting('statement_timeout'),'source',(SELECT source FROM pg_settings WHERE name='statement_timeout')); ROLLBACK; BEGIN READ ONLY; SELECT jsonb_build_object('phase','after_rollback','timeout',current_setting('statement_timeout'),'source',(SELECT source FROM pg_settings WHERE name='statement_timeout'));")
run('full_validator_timing',"EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT public.taxonomy_v2_validate_release('tax-2026.09.30-01');")
f=Path.home()/'sporely-scratch/vernacular-2026-10-07-f1'
rows=[json.loads(x) for x in (f/'run1/scoped/taxon.jsonl').read_text().splitlines()]
ids=sorted(x['taxon_id'] for x in rows);positions={x:i+1 for i,x in enumerate(ids)}
comparisons=sum(positions.get(x['parent_taxon_id'],len(ids)) for x in rows if x['parent_taxon_id'] is not None)
print('frozen candidate count',len(rows),'all parents nonnull',sum(x['parent_taxon_id'] is not None for x in rows),'full repeated release index scan comparisons',comparisons)
S=Path.home()/'sporely-scratch/vernacular-2026-10-07-stage4/timeout-investigation'
(S/'candidate-plan-bound.json').write_text(json.dumps({'candidate_taxa':len(rows),'nonnull_parents':sum(x['parent_taxon_id'] is not None for x in rows),'missing_parents':sum(x['parent_taxon_id'] is not None and x['parent_taxon_id'] not in positions for x in rows),'comparisons_if_repeated_release_only_ascending_index_scan':comparisons,'note':'Structural estimate for observed custom plan, not a measured execution time or captured failed-session plan.'},indent=2))
