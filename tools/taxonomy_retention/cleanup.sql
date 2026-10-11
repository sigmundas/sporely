-- REVIEW CANDIDATE ONLY. Production execution requires explicit approval.
-- Run with psql -X -v ON_ERROR_STOP=1 -f cleanup.sql.
-- Permanent release headers, concepts, import audits and v3 history survive.
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';
SELECT pg_advisory_xact_lock(846920026072413002::bigint);
CREATE TEMP TABLE cleanup_targets(release_id text PRIMARY KEY) ON COMMIT DROP;
INSERT INTO cleanup_targets VALUES
  ('tax-2026.08.01-01'), ('tax-2026.09.26-02'), ('tax-2026.09.30-01');
LOCK TABLE public.taxonomy_v2_releases IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.taxonomy_v2_taxa, public.taxonomy_v2_scientific_names,
  public.taxonomy_v2_vernacular_names, public.taxonomy_v2_external_ids,
  public.taxonomy_v2_legacy_external_ids, public.taxonomy_v2_redlist
  IN SHARE ROW EXCLUSIVE MODE;
DO $$
BEGIN
  IF (SELECT count(*) FROM public.taxonomy_v2_releases WHERE status='active') <> 1
     OR NOT EXISTS (SELECT 1 FROM public.taxonomy_v2_releases
       WHERE release_id='tax-2026.10.07-01' AND status='active') THEN
    RAISE EXCEPTION 'Unexpected active release; re-review required';
  END IF;
  IF (SELECT count(*) FROM cleanup_targets c JOIN public.taxonomy_v2_releases r
       USING(release_id) WHERE r.status='retired') <> 3 THEN
    RAISE EXCEPTION 'Every selected release must exist and be retired';
  END IF;
END $$;
-- Bulk child deletion avoids per-parent cascade scans of retained children.
DELETE FROM public.taxonomy_v2_scientific_names WHERE release_id IN (SELECT release_id FROM cleanup_targets);
DELETE FROM public.taxonomy_v2_vernacular_names WHERE release_id IN (SELECT release_id FROM cleanup_targets);
DELETE FROM public.taxonomy_v2_external_ids WHERE release_id IN (SELECT release_id FROM cleanup_targets);
DELETE FROM public.taxonomy_v2_legacy_external_ids WHERE release_id IN (SELECT release_id FROM cleanup_targets);
DELETE FROM public.taxonomy_v2_redlist WHERE release_id IN (SELECT release_id FROM cleanup_targets);
DELETE FROM public.taxonomy_v2_taxa WHERE release_id IN (SELECT release_id FROM cleanup_targets);
DO $$
DECLARE t text; remaining bigint;
BEGIN
  FOREACH t IN ARRAY ARRAY['taxonomy_v2_taxa','taxonomy_v2_scientific_names',
    'taxonomy_v2_vernacular_names','taxonomy_v2_external_ids',
    'taxonomy_v2_legacy_external_ids','taxonomy_v2_redlist'] LOOP
    EXECUTE format('SELECT count(*) FROM public.%I WHERE release_id IN (SELECT release_id FROM cleanup_targets)',t) INTO remaining;
    IF remaining <> 0 THEN RAISE EXCEPTION 'Payload remains in %',t; END IF;
  END LOOP;
  IF NOT coalesce((public.taxonomy_v2_validate_release('tax-2026.10.07-01')->>'ok')::boolean,false) THEN
    RAISE EXCEPTION 'Active release integrity failed';
  END IF;
END $$;
COMMIT;
