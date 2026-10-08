BEGIN READ ONLY;
PREPARE parent_check(text) AS SELECT count(*) FROM public.taxonomy_v2_taxa t WHERE t.release_id=$1 AND t.parent_sporely_taxon_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_taxa p WHERE p.release_id=t.release_id AND p.sporely_taxon_id=t.parent_sporely_taxon_id); SET LOCAL plan_cache_mode=force_custom_plan; EXPLAIN (VERBOSE,FORMAT JSON) EXECUTE parent_check('tax-2026.10.07-01'); SET LOCAL plan_cache_mode=force_generic_plan; EXPLAIN (ANALYZE,BUFFERS,VERBOSE,SETTINGS,FORMAT JSON) EXECUTE parent_check('tax-2026.09.30-01'); DEALLOCATE parent_check;
ROLLBACK;
