BEGIN READ ONLY;
SELECT 'releases ' || string_agg(release_id || '=' || status, ', ' ORDER BY release_id) FROM public.taxonomy_v2_releases WHERE release_id IN ('tax-2026.10.23-11','tax-2026.10.31-01');
SELECT 'active ' || string_agg(release_id, ',') FROM public.taxonomy_v2_releases WHERE status='active';
SELECT 'validate_ok ' || (public.taxonomy_v2_validate_release('tax-2026.10.31-01')->>'ok');
SELECT 'national_name_errors ' || public.taxonomy_v2_national_name_errors('tax-2026.10.31-01')::text;
SELECT 'import_run ' || status || ' ' || counts::text FROM public.taxonomy_v2_import_runs WHERE release_id='tax-2026.10.31-01';
SELECT 'counts taxa=' || (SELECT count(*) FROM public.taxonomy_v2_taxa WHERE release_id='tax-2026.10.31-01')
  || ' sci=' || (SELECT count(*) FROM public.taxonomy_v2_scientific_names WHERE release_id='tax-2026.10.31-01')
  || ' vern=' || (SELECT count(*) FROM public.taxonomy_v2_vernacular_names WHERE release_id='tax-2026.10.31-01')
  || ' ext=' || (SELECT count(*) FROM public.taxonomy_v2_external_ids WHERE release_id='tax-2026.10.31-01')
  || ' redlist=' || (SELECT count(*) FROM public.taxonomy_v2_redlist WHERE release_id='tax-2026.10.31-01');
SELECT 'r11_kept taxa=' || (SELECT count(*) FROM public.taxonomy_v2_taxa WHERE release_id='tax-2026.10.23-11');
SELECT 'nb_' || q || ' ' || coalesce((SELECT taxon_id::text FROM public.search_taxa_v2(q,'no',5) LIMIT 1),'NONE') FROM (VALUES ('kantareller'),('eggrøyksopper'),('fingersopper'),('fagerkjuker'),('beitemarkvokssopper'),('silkeparasollsopp'),('kalktalg')) v(q);
SELECT 'col_' || c || ' ' || coalesce((SELECT string_agg(taxon_id::text,',') FROM public.resolve_taxon_external_id_v2('col_xr','col_usage_id',c)),'NONE') FROM (VALUES ('XQVS'),('XQVT')) v(c);
SELECT 'nortaxa_226896 ' || coalesce((SELECT string_agg(taxon_id::text,',') FROM public.resolve_taxon_external_id_v2('nortaxa','nortaxa_taxon_id','226896')),'NONE');
SELECT 'dyntaxa_3213 ' || coalesce((SELECT string_agg(taxon_id::text,',') FROM public.resolve_taxon_external_id_v2('dyntaxa','dyntaxa_taxon_id','urn:lsid:dyntaxa.se:Taxon:3213')),'NONE');
SELECT 'selected_not_in_active ' || count(DISTINCT o.selected_sporely_taxon_id) FROM public.observations o WHERE o.selected_sporely_taxon_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_taxa t JOIN public.taxonomy_v2_releases r USING (release_id) WHERE r.status='active' AND t.sporely_taxon_id=o.selected_sporely_taxon_id);
SELECT 'idle_in_transaction ' || count(*) FROM pg_stat_activity WHERE state LIKE 'idle in transaction%';
ROLLBACK;
