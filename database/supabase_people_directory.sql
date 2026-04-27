-- ==============================================================================
-- People Directory RPC
-- Exposes a privacy-aware contributor directory for the web People screen.
-- Intended to be executed manually in the Supabase SQL Editor.
-- ==============================================================================

CREATE OR REPLACE FUNCTION public.search_people_directory(
  p_query text DEFAULT NULL,
  p_limit int DEFAULT 24
)
RETURNS TABLE (
  user_id uuid,
  username text,
  display_name text,
  bio text,
  avatar_url text,
  public_find_count bigint,
  public_species_count bigint,
  public_spore_count bigint,
  latest_public_observation_at timestamptz
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  WITH normalized AS (
    SELECT
      nullif(btrim(coalesce(p_query, '')), '') AS q,
      greatest(1, least(coalesce(p_limit, 24), 100)) AS lim
  ),
  visible_profiles AS (
    SELECT
      p.id,
      p.username,
      p.display_name,
      p.bio,
      p.avatar_url
    FROM public.profiles p
    CROSS JOIN normalized n
    WHERE p.is_banned IS NOT TRUE
      AND NOT EXISTS (
        SELECT 1
        FROM public.user_blocks ub
        WHERE (ub.blocker_id = auth.uid() AND ub.blocked_id = p.id)
           OR (ub.blocker_id = p.id AND ub.blocked_id = auth.uid())
      )
      AND (
        n.q IS NULL
        OR coalesce(p.username, '') ILIKE '%' || n.q || '%'
        OR coalesce(p.display_name, '') ILIKE '%' || n.q || '%'
      )
  ),
  public_observations AS (
    SELECT
      o.id,
      o.user_id,
      o.created_at,
      o.genus,
      o.species,
      o.spore_data_visibility
    FROM public.observations_community_view o
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
      ) AS public_species_count,
      max(po.created_at) AS latest_public_observation_at
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
    vp.id AS user_id,
    vp.username,
    vp.display_name,
    vp.bio,
    vp.avatar_url,
    coalesce(pos.public_find_count, 0) AS public_find_count,
    coalesce(pos.public_species_count, 0) AS public_species_count,
    coalesce(ps.public_spore_count, 0) AS public_spore_count,
    pos.latest_public_observation_at
  FROM visible_profiles vp
  LEFT JOIN public_observation_stats pos
    ON pos.user_id = vp.id
  LEFT JOIN public_spore_stats ps
    ON ps.user_id = vp.id
  CROSS JOIN normalized n
  WHERE n.q IS NOT NULL
     OR pos.latest_public_observation_at IS NOT NULL
  ORDER BY
    CASE
      WHEN n.q IS NULL THEN 0
      WHEN lower(coalesce(vp.username, '')) = lower(n.q) THEN 0
      WHEN lower(coalesce(vp.display_name, '')) = lower(n.q) THEN 1
      WHEN lower(coalesce(vp.username, '')) LIKE lower(n.q) || '%' THEN 2
      WHEN lower(coalesce(vp.display_name, '')) LIKE lower(n.q) || '%' THEN 3
      ELSE 4
    END,
    pos.latest_public_observation_at DESC NULLS LAST,
    coalesce(vp.display_name, vp.username, '') ASC
  LIMIT (SELECT lim FROM normalized);
$$;

REVOKE ALL ON FUNCTION public.search_people_directory(text, int) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.search_people_directory(text, int) TO authenticated;
