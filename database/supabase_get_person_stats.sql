-- ==============================================================================
-- Get Person Stats RPC
-- Quickly fetches public finds, species, and spores counts for a single user.
-- ==============================================================================

CREATE OR REPLACE FUNCTION public.get_person_stats(p_user_id uuid)
RETURNS TABLE (
  user_id uuid,
  public_find_count bigint,
  public_species_count bigint,
  public_spore_count bigint
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  WITH public_observations AS (
    SELECT
      o.id,
      o.user_id,
      o.genus,
      o.species,
      o.spore_data_visibility
    FROM public.observations_community_view o
    WHERE o.user_id = p_user_id
  ),
  public_observation_stats AS (
    SELECT
      po.user_id,
      count(distinct po.id) AS public_find_count,
      count(
        distinct CASE
          WHEN nullif(trim(coalesce(po.genus, '')), '') IS NOT NULL
            OR nullif(trim(coalesce(po.species, '')), '') IS NOT NULL
          THEN lower(trim(coalesce(po.genus, ''))) || '|' || lower(trim(coalesce(po.species, '')))
          ELSE NULL
        END
      ) AS public_species_count
    FROM public_observations po
    GROUP BY po.user_id
  ),
  public_spore_stats AS (
    SELECT
      po.user_id,
      count(*) AS public_spore_count
    FROM public_observations po
    JOIN public.observation_images i
      ON i.observation_id = po.id
    JOIN public.spore_measurements m
      ON m.image_id = i.id
    WHERE coalesce(po.spore_data_visibility, 'public') = 'public'
      AND (
        m.measurement_type IS NULL
        OR m.measurement_type = ''
        OR lower(m.measurement_type) IN ('manual', 'spore', 'spores')
      )
    GROUP BY po.user_id
  )
  SELECT
    p_user_id AS user_id,
    coalesce(pos.public_find_count, 0) AS public_find_count,
    coalesce(pos.public_species_count, 0) AS public_species_count,
    coalesce(ps.public_spore_count, 0) AS public_spore_count
  FROM (SELECT p_user_id AS id) vp
  LEFT JOIN public_observation_stats pos
    ON pos.user_id = vp.id
  LEFT JOIN public_spore_stats ps
    ON ps.user_id = vp.id;
$$;

REVOKE ALL ON FUNCTION public.get_person_stats(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.get_person_stats(uuid) TO authenticated;

