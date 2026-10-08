BEGIN READ ONLY;
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, FORMAT JSON) SELECT count(*) FROM public.taxonomy_v2_taxa t WHERE t.release_id = 'tax-2026.09.30-01' AND t.parent_sporely_taxon_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_taxa p WHERE p.release_id=t.release_id AND p.sporely_taxon_id=t.parent_sporely_taxon_id);
ROLLBACK;
