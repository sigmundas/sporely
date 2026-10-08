from pathlib import Path
import subprocess,json,datetime
S=Path.home()/'sporely-scratch/vernacular-2026-10-07-stage4/timeout-investigation'
S.mkdir(exist_ok=True)
def run(name,sql):
 p=subprocess.run(['docker','run','--rm','-i','--env-file',str(Path.home()/'.config/sporely/production-db.env'),'postgres:17','sh','-c','psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -Atf -'],input='BEGIN READ ONLY;\n'+sql+'\nROLLBACK;\n',text=True,capture_output=True)
 (S/(name+'.sql')).write_text('BEGIN READ ONLY;\n'+sql+'\nROLLBACK;\n');(S/(name+'.out')).write_text(p.stdout);(S/(name+'.err')).write_text(p.stderr)
 print(name,'exit',p.returncode,p.stdout[:15000],p.stderr[:1500],flush=True)
 return p
if __name__=='__main__':
 run('settings',"""SELECT jsonb_build_object('time',clock_timestamp(),'user',current_user,'session_user',session_user,'database',current_database(),'settings',(SELECT jsonb_agg(to_jsonb(s)) FROM (SELECT name,setting,unit,source,sourcefile,sourceline,boot_val,reset_val,context FROM pg_settings WHERE name IN ('statement_timeout','plan_cache_mode','work_mem','jit','default_statistics_target')) s),'role_database_settings',(SELECT jsonb_agg(jsonb_build_object('role',coalesce(r.rolname,'ALL'),'database',coalesce(d.datname,'ALL'),'settings',s.setconfig)) FROM pg_db_role_setting s LEFT JOIN pg_roles r ON r.oid=s.setrole LEFT JOIN pg_database d ON d.oid=s.setdatabase WHERE s.setrole IN (0,(SELECT oid FROM pg_roles WHERE rolname=current_user))),'function_settings',(SELECT jsonb_agg(jsonb_build_object('name',p.oid::regprocedure::text,'settings',proconfig)) FROM pg_proc p WHERE p.oid IN ('public.taxonomy_v2_validate_release(text)'::regprocedure,'public.taxonomy_v2_activate_release(text)'::regprocedure)));
 SELECT jsonb_build_object('statistics',(SELECT row_to_json(s) FROM (SELECT n_live_tup,n_dead_tup,last_analyze,last_autoanalyze,n_mod_since_analyze FROM pg_stat_user_tables WHERE relname='taxonomy_v2_taxa') s),'indexes',(SELECT jsonb_agg(indexdef) FROM pg_indexes WHERE schemaname='public' AND tablename='taxonomy_v2_taxa'),'release_stats',(SELECT jsonb_agg(to_jsonb(s)) FROM (SELECT attname,n_distinct,most_common_vals::text,most_common_freqs FROM pg_stats WHERE schemaname='public' AND tablename='taxonomy_v2_taxa' AND attname IN ('release_id','parent_sporely_taxon_id','sporely_taxon_id')) s));
 SELECT pg_get_functiondef('public.taxonomy_v2_validate_release(text)'::regprocedure);""")
 q="SELECT count(*) FROM public.taxonomy_v2_taxa t WHERE t.release_id = '%s' AND t.parent_sporely_taxon_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_taxa p WHERE p.release_id=t.release_id AND p.sporely_taxon_id=t.parent_sporely_taxon_id)"
 run('plan_target_absent','EXPLAIN (VERBOSE, COSTS, FORMAT JSON) '+q%'tax-2026.10.07-01'+';')
 run('plan_active_measured','EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, FORMAT JSON) '+q%'tax-2026.09.30-01'+';')
