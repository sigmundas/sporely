-- W3-B final taxonomy-field drift export.
--
-- READ-ONLY. Single SELECT. No writes, no locks beyond SELECT, no elevated
-- role, no server-side extension.
--
-- Returns exactly the same set of taxonomy columns w3a/w3a2 already read,
-- for every observation whose id is referenced by the deployment manifest.
-- The operator downloads this as CSV from the Supabase SQL Editor and feeds
-- it to build_deployment_manifest.py --observations-fingerprint <path> to
-- compare each row's current fingerprint against the fingerprint stamped at
-- export time.
--
-- The list of real observation IDs must be pasted in place of __IDS__ before
-- running the query. The operator receives this list from the deployment
-- manifest's real_observation_id column — it never appears in Git.

SELECT
    id,
    artsdata_id,
    artportalen_id,
    inaturalist_id,
    mushroomobserver_id,
    desktop_id,
    ai_selected_service,
    ai_selected_taxon_id,
    ai_selected_scientific_name,
    genus,
    species,
    common_name,
    species_guess
FROM public.observations
WHERE id = ANY (ARRAY[__IDS__]::bigint[])
ORDER BY id;
