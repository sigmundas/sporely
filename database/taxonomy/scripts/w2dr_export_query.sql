-- W2D-R historical reconciliation export
--
-- READ-ONLY. This file contains a single SELECT statement. It:
--   * writes to no table, no schema, no function;
--   * calls no INSERT / UPDATE / DELETE / TRUNCATE / MERGE / COPY / EXECUTE / DO;
--   * takes no locks beyond what SELECT itself requires;
--   * uses no elevated role and no server-side extension;
--   * returns only columns needed for taxonomy reconciliation.
--
-- Excluded on purpose (privacy):
--   user_id, date, location, gps_latitude, gps_longitude, gps_altitude,
--   gps_accuracy, location_precision, location_public, habitat*, notes,
--   open_comment, private_comment, image_key, thumb_key, author, citation,
--   captured_at, created_at, updated_at, synced_at, is_draft, source_type,
--   publish_target, visibility, data_provider, spore_data_visibility,
--   spore_statistics, uncertain, unspontaneous, determination_method,
--   interesting_comment, ai_state_json, ai_selected_probability,
--   ai_selected_at, auto_threshold, red_list_category, red_list_categories_json.
--
-- The `id` column returned here is the raw bigint observation id and MUST
-- be pseudonymised before it leaves the operator's machine. The offline
-- transformer performs that pseudonymisation; do NOT commit the raw output
-- of this query.

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
ORDER BY id;
