from pathlib import Path
S=Path.home()/'sporely-scratch/vernacular-2026-10-07-stage4'
exec((S/'preflight.py').read_text().split("report={'start_utc'")[0])
base=json.loads((S/'preflight.json').read_text());report={'verdict':'STOP_PRODUCTION_MISMATCH','checks':{},'checked_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
for table in ['taxonomy_v2_taxa','taxonomy_v2_scientific_names','taxonomy_v2_external_ids','taxonomy_v2_redlist']:
 rows=query(f"SELECT to_jsonb(t)-'release_id' FROM public.{table} t WHERE release_id='{PREV}';")
 report['checks'][table]={'count':len(rows),'sha256':digest(rows),'unchanged':digest(rows)==base['checks'][table]['before_sha256']}
for table in ['registry_concept','external_mapping','identification_snapshot','resolution_link','release_installation','supplement_installation','reconciliation_manifest_audit']:
 rows=query(f'SELECT to_jsonb(t) FROM taxonomy_v3.{table} t;')
 report['checks']['taxonomy_v3.'+table]={'count':len(rows),'sha256':digest(rows),'unchanged':digest(rows)==base['checks']['taxonomy_v3.'+table]['before_sha256']}
report['all_invariants_unchanged']=all(x['unchanged'] for x in report['checks'].values())
(S/'rollback-verification.json').write_text(json.dumps(report,indent=2));print(json.dumps(report,indent=2));assert report['all_invariants_unchanged']
