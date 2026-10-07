from preflight import *
r=query(f"""SELECT jsonb_build_object('per_release',(SELECT jsonb_object_agg(rel,n) FROM (SELECT t.release_id rel,count(*) n FROM public.taxonomy_v2_taxa t WHERE t.parent_sporely_taxon_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_taxa p WHERE p.release_id=t.release_id AND p.sporely_taxon_id=t.parent_sporely_taxon_id OFFSET 0) GROUP BY 1) x),
'rows',(SELECT jsonb_agg(to_jsonb(t)) FROM public.taxonomy_v2_taxa t WHERE t.release_id='{RID}' AND t.parent_sporely_taxon_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_taxa p WHERE p.release_id=t.release_id AND p.sporely_taxon_id=t.parent_sporely_taxon_id OFFSET 0)));""")[0]
print(json.dumps(r,ensure_ascii=False,indent=1))
