-- Community spore-data SQL draft aligned to the current Supabase schema.
-- Apply in small batches in the Supabase SQL editor and adjust based on
-- existing RLS policies and any production data issues.
--
-- This version builds on the current tables:
-- - public.observations
-- - public.observation_images
-- - public.spore_measurements
-- - public.calibrations
-- - public.profiles
-- - public.friendships
-- - public.observation_shares
-- - public.reference_values
--
-- Key idea:
-- - observation visibility remains unchanged
-- - location visibility remains unchanged
-- - spore-data visibility becomes a separate field on observations
-- - public/community access is exposed through SECURITY DEFINER RPCs
-- - exact GPS, location text, and private notes are not returned
--
-- Notes from current production RLS:
-- - observations: owner full, friends read; no direct public SELECT policy
-- - observation_images: owner full, friends read, plus a public-image path via
--   observation visibility
-- - spore_measurements: owner full, friends read; no public SELECT policy
-- - reference_values: authenticated read
-- - profiles: authenticated public read
-- - existing views:
--   - observations_community_view exposes location, notes, and public GPS
--   - observations_friend_view exposes notes/open_comment and conditional GPS
--
-- Result:
-- - direct public querying of observations/spore_measurements is not available
-- - SECURITY DEFINER RPCs are the cleanest way to expose safe public spore data
-- - these RPCs should be granted to authenticated users unless you later choose
--   to support anonymous community search
-- - the existing observation views should not be reused for community spore
--   review/import because they expose more than this feature should surface

-- ── 1. Schema updates ────────────────────────────────────────────────────────

ALTER TABLE public.observations
  ADD COLUMN IF NOT EXISTS spore_data_visibility text DEFAULT 'public';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'observations_spore_data_visibility_check'
  ) THEN
    ALTER TABLE public.observations
      ADD CONSTRAINT observations_spore_data_visibility_check
      CHECK (spore_data_visibility IN ('private', 'friends', 'public'));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_observations_spore_visibility
  ON public.observations (spore_data_visibility);

CREATE INDEX IF NOT EXISTS idx_observations_species_spore_visibility
  ON public.observations (genus, species, spore_data_visibility);

CREATE INDEX IF NOT EXISTS idx_spore_measurements_image_type
  ON public.spore_measurements (image_id, measurement_type);

CREATE INDEX IF NOT EXISTS idx_observation_images_observation_id
  ON public.observation_images (observation_id);

CREATE INDEX IF NOT EXISTS idx_calibrations_user_objective
  ON public.calibrations (user_id, objective_key, is_active, calibration_date DESC);

-- ── 2. Helper functions ──────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.are_friends(user_a uuid, user_b uuid)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT EXISTS (
    SELECT 1
    FROM public.friendships f
    WHERE f.status = 'accepted'
      AND (
        (f.requester_id = user_a AND f.addressee_id = user_b)
        OR
        (f.requester_id = user_b AND f.addressee_id = user_a)
      )
  )
$$;

CREATE OR REPLACE FUNCTION public.can_access_spore_data(
  owner_id uuid,
  spore_visibility text
)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT CASE
    WHEN auth.uid() = owner_id THEN true
    WHEN coalesce(spore_visibility, 'public') = 'public' THEN true
    WHEN coalesce(spore_visibility, 'public') = 'friends'
      THEN public.are_friends(auth.uid(), owner_id)
    ELSE false
  END
$$;

CREATE OR REPLACE FUNCTION public.community_contributor_label(
  profile_id uuid,
  fallback_author text DEFAULT NULL
)
RETURNS text
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT nullif(
    coalesce(
      nullif(p.display_name, ''),
      nullif(p.username, ''),
      nullif(fallback_author, '')
    ),
    ''
  )
  FROM public.profiles p
  WHERE p.id = profile_id
  UNION ALL
  SELECT nullif(fallback_author, '')
  WHERE NOT EXISTS (
    SELECT 1 FROM public.profiles p WHERE p.id = profile_id
  )
  LIMIT 1
$$;

-- ── 3. Search observation-backed community spore datasets ───────────────────

CREATE OR REPLACE FUNCTION public.search_community_spore_datasets(
  p_genus text,
  p_species text,
  p_limit int DEFAULT 50
)
RETURNS TABLE (
  dataset_type text,
  observation_id bigint,
  genus text,
  species text,
  contributor_label text,
  observed_on date,
  measurement_count bigint,
  image_count bigint,
  length_min double precision,
  length_p05 double precision,
  length_p50 double precision,
  length_p95 double precision,
  length_max double precision,
  width_min double precision,
  width_p05 double precision,
  width_p50 double precision,
  width_p95 double precision,
  width_max double precision,
  q_min double precision,
  q_p50 double precision,
  q_max double precision,
  qc_flags jsonb
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  WITH filtered AS (
    SELECT
      o.id AS observation_id,
      o.user_id,
      o.genus,
      o.species,
      o.date,
      o.author,
      i.id AS image_id,
      i.mount_medium,
      i.stain,
      i.sample_type,
      i.contrast,
      i.objective_name,
      i.scale_microns_per_pixel,
      m.id AS measurement_id,
      m.length_um,
      m.width_um,
      m.p1_x,
      m.p1_y,
      m.p2_x,
      m.p2_y,
      m.p3_x,
      m.p3_y,
      m.p4_x,
      m.p4_y
    FROM public.observations o
    JOIN public.observation_images i
      ON i.observation_id = o.id
    JOIN public.spore_measurements m
      ON m.image_id = i.id
    WHERE lower(coalesce(o.genus, '')) = lower(trim(coalesce(p_genus, '')))
      AND (trim(coalesce(p_species, '')) = '' OR lower(coalesce(o.species, '')) = lower(trim(p_species)))
      AND public.can_access_spore_data(o.user_id, o.spore_data_visibility)
      AND m.length_um IS NOT NULL
      AND m.width_um IS NOT NULL
      AND (
        m.measurement_type IS NULL
        OR m.measurement_type = ''
        OR lower(m.measurement_type) IN ('manual', 'spore', 'spores')
      )
  )
  SELECT
    'observation'::text AS dataset_type,
    f.observation_id,
    max(f.genus) AS genus,
    max(f.species) AS species,
    public.community_contributor_label((array_agg(f.user_id))[1], max(f.author)) AS contributor_label,
    max(f.date) AS observed_on,
    count(f.measurement_id) AS measurement_count,
    count(distinct f.image_id) AS image_count,
    min(f.length_um) AS length_min,
    percentile_cont(0.05) WITHIN GROUP (ORDER BY f.length_um)::double precision AS length_p05,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY f.length_um)::double precision AS length_p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY f.length_um)::double precision AS length_p95,
    max(f.length_um) AS length_max,
    min(f.width_um) AS width_min,
    percentile_cont(0.05) WITHIN GROUP (ORDER BY f.width_um)::double precision AS width_p05,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY f.width_um)::double precision AS width_p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY f.width_um)::double precision AS width_p95,
    max(f.width_um) AS width_max,
    min(f.length_um / nullif(f.width_um, 0)) AS q_min,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY (f.length_um / nullif(f.width_um, 0)))::double precision AS q_p50,
    max(f.length_um / nullif(f.width_um, 0)) AS q_max,
    jsonb_build_object(
      'has_mount', bool_or(nullif(f.mount_medium, '') IS NOT NULL),
      'has_stain', bool_or(nullif(f.stain, '') IS NOT NULL),
      'has_sample_type', bool_or(nullif(f.sample_type, '') IS NOT NULL),
      'has_contrast', bool_or(nullif(f.contrast, '') IS NOT NULL),
      'has_objective', bool_or(nullif(f.objective_name, '') IS NOT NULL),
      'has_scale', bool_or(f.scale_microns_per_pixel IS NOT NULL),
      'has_point_geometry', bool_or(
        f.p1_x IS NOT NULL OR f.p1_y IS NOT NULL OR f.p2_x IS NOT NULL OR f.p2_y IS NOT NULL
      ),
      'measurement_count', count(f.measurement_id)
    ) AS qc_flags
  FROM filtered f
  GROUP BY f.observation_id
  ORDER BY
    count(f.measurement_id) DESC,
    max(f.date) DESC,
    f.observation_id DESC
  LIMIT greatest(coalesce(p_limit, 50), 1)
$$;

-- ── 4. Detailed observation-backed review RPC ────────────────────────────────

CREATE OR REPLACE FUNCTION public.get_community_spore_dataset(
  p_observation_id bigint
)
RETURNS TABLE (
  dataset_type text,
  observation_id bigint,
  genus text,
  species text,
  common_name text,
  contributor_label text,
  observed_on date,
  measurement_count bigint,
  image_count bigint,
  mount_media text[],
  stains text[],
  sample_types text[],
  contrasts text[],
  objectives text[],
  scale_min double precision,
  scale_max double precision,
  qc_flags jsonb,
  length_min double precision,
  length_p05 double precision,
  length_p50 double precision,
  length_p95 double precision,
  length_max double precision,
  length_avg double precision,
  width_min double precision,
  width_p05 double precision,
  width_p50 double precision,
  width_p95 double precision,
  width_max double precision,
  width_avg double precision,
  q_min double precision,
  q_p50 double precision,
  q_max double precision,
  q_avg double precision,
  measurements_json jsonb
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  WITH filtered AS (
    SELECT
      o.id AS observation_id,
      o.user_id,
      o.genus,
      o.species,
      o.common_name,
      o.date,
      o.author,
      i.id AS image_id,
      i.mount_medium,
      i.stain,
      i.sample_type,
      i.contrast,
      i.objective_name,
      i.scale_microns_per_pixel,
      m.id AS measurement_id,
      m.length_um,
      m.width_um,
      m.p1_x,
      m.p1_y,
      m.p2_x,
      m.p2_y,
      m.p3_x,
      m.p3_y,
      m.p4_x,
      m.p4_y,
      m.measured_at
    FROM public.observations o
    JOIN public.observation_images i
      ON i.observation_id = o.id
    JOIN public.spore_measurements m
      ON m.image_id = i.id
    WHERE o.id = p_observation_id
      AND public.can_access_spore_data(o.user_id, o.spore_data_visibility)
      AND m.length_um IS NOT NULL
      AND m.width_um IS NOT NULL
      AND (
        m.measurement_type IS NULL
        OR m.measurement_type = ''
        OR lower(m.measurement_type) IN ('manual', 'spore', 'spores')
      )
  )
  SELECT
    'observation'::text AS dataset_type,
    max(f.observation_id) AS observation_id,
    max(f.genus) AS genus,
    max(f.species) AS species,
    max(f.common_name) AS common_name,
    public.community_contributor_label((array_agg(f.user_id))[1], max(f.author)) AS contributor_label,
    max(f.date) AS observed_on,
    count(f.measurement_id) AS measurement_count,
    count(distinct f.image_id) AS image_count,
    array_remove(array_agg(distinct nullif(f.mount_medium, '')), NULL) AS mount_media,
    array_remove(array_agg(distinct nullif(f.stain, '')), NULL) AS stains,
    array_remove(array_agg(distinct nullif(f.sample_type, '')), NULL) AS sample_types,
    array_remove(array_agg(distinct nullif(f.contrast, '')), NULL) AS contrasts,
    array_remove(array_agg(distinct nullif(f.objective_name, '')), NULL) AS objectives,
    min(f.scale_microns_per_pixel) AS scale_min,
    max(f.scale_microns_per_pixel) AS scale_max,
    jsonb_build_object(
      'has_mount', bool_or(nullif(f.mount_medium, '') IS NOT NULL),
      'has_stain', bool_or(nullif(f.stain, '') IS NOT NULL),
      'has_sample_type', bool_or(nullif(f.sample_type, '') IS NOT NULL),
      'has_contrast', bool_or(nullif(f.contrast, '') IS NOT NULL),
      'has_objective', bool_or(nullif(f.objective_name, '') IS NOT NULL),
      'has_scale', bool_or(f.scale_microns_per_pixel IS NOT NULL),
      'has_point_geometry', bool_or(
        f.p1_x IS NOT NULL OR f.p1_y IS NOT NULL OR f.p2_x IS NOT NULL OR f.p2_y IS NOT NULL
      ),
      'measurement_count', count(f.measurement_id)
    ) AS qc_flags,
    min(f.length_um) AS length_min,
    percentile_cont(0.05) WITHIN GROUP (ORDER BY f.length_um)::double precision AS length_p05,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY f.length_um)::double precision AS length_p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY f.length_um)::double precision AS length_p95,
    max(f.length_um) AS length_max,
    avg(f.length_um) AS length_avg,
    min(f.width_um) AS width_min,
    percentile_cont(0.05) WITHIN GROUP (ORDER BY f.width_um)::double precision AS width_p05,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY f.width_um)::double precision AS width_p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY f.width_um)::double precision AS width_p95,
    max(f.width_um) AS width_max,
    avg(f.width_um) AS width_avg,
    min(f.length_um / nullif(f.width_um, 0)) AS q_min,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY (f.length_um / nullif(f.width_um, 0)))::double precision AS q_p50,
    max(f.length_um / nullif(f.width_um, 0)) AS q_max,
    avg(f.length_um / nullif(f.width_um, 0)) AS q_avg,
    jsonb_agg(
      jsonb_build_object(
        'measurement_id', f.measurement_id,
        'image_id', f.image_id,
        'length_um', f.length_um,
        'width_um', f.width_um,
        'p1_x', f.p1_x,
        'p1_y', f.p1_y,
        'p2_x', f.p2_x,
        'p2_y', f.p2_y,
        'p3_x', f.p3_x,
        'p3_y', f.p3_y,
        'p4_x', f.p4_x,
        'p4_y', f.p4_y,
        'measured_at', f.measured_at
      )
      ORDER BY f.measured_at, f.measurement_id
    ) AS measurements_json
  FROM filtered f
$$;

-- ── 5. Aggregate public community stats by taxon ────────────────────────────

CREATE OR REPLACE FUNCTION public.community_spore_taxon_summary(
  p_genus text,
  p_species text
)
RETURNS TABLE (
  dataset_count bigint,
  measurement_count bigint,
  length_min double precision,
  length_p05 double precision,
  length_p50 double precision,
  length_p95 double precision,
  length_max double precision,
  length_avg double precision,
  width_min double precision,
  width_p05 double precision,
  width_p50 double precision,
  width_p95 double precision,
  width_max double precision,
  width_avg double precision,
  q_min double precision,
  q_p50 double precision,
  q_max double precision,
  q_avg double precision
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  WITH public_points AS (
    SELECT
      o.id AS observation_id,
      m.length_um,
      m.width_um,
      (m.length_um / nullif(m.width_um, 0)) AS q_value
    FROM public.observations o
    JOIN public.observation_images i
      ON i.observation_id = o.id
    JOIN public.spore_measurements m
      ON m.image_id = i.id
    WHERE lower(coalesce(o.genus, '')) = lower(trim(coalesce(p_genus, '')))
      AND (trim(coalesce(p_species, '')) = '' OR lower(coalesce(o.species, '')) = lower(trim(p_species)))
      AND o.spore_data_visibility = 'public'
      AND m.length_um IS NOT NULL
      AND m.width_um IS NOT NULL
      AND m.width_um <> 0
      AND (
        m.measurement_type IS NULL
        OR m.measurement_type = ''
        OR lower(m.measurement_type) IN ('manual', 'spore', 'spores')
      )
  )
  SELECT
    count(distinct observation_id) AS dataset_count,
    count(*) AS measurement_count,
    min(length_um) AS length_min,
    percentile_cont(0.05) WITHIN GROUP (ORDER BY length_um)::double precision AS length_p05,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY length_um)::double precision AS length_p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY length_um)::double precision AS length_p95,
    max(length_um) AS length_max,
    avg(length_um) AS length_avg,
    min(width_um) AS width_min,
    percentile_cont(0.05) WITHIN GROUP (ORDER BY width_um)::double precision AS width_p05,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY width_um)::double precision AS width_p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY width_um)::double precision AS width_p95,
    max(width_um) AS width_max,
    avg(width_um) AS width_avg,
    min(q_value) AS q_min,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY q_value)::double precision AS q_p50,
    max(q_value) AS q_max,
    avg(q_value) AS q_avg
  FROM public_points
$$;

-- ── 6. Search public reference values ───────────────────────────────────────
--
-- Current caveat:
-- public.reference_values has no user_id / provenance / privacy fields.
-- This RPC treats the table as globally readable reference material.

CREATE OR REPLACE FUNCTION public.search_public_reference_values(
  p_genus text,
  p_species text,
  p_limit int DEFAULT 50
)
RETURNS TABLE (
  reference_id bigint,
  genus text,
  species text,
  source text,
  mount_medium text,
  stain text,
  length_min double precision,
  length_p05 double precision,
  length_p50 double precision,
  length_p95 double precision,
  length_max double precision,
  width_min double precision,
  width_p05 double precision,
  width_p50 double precision,
  width_p95 double precision,
  width_max double precision,
  q_min double precision,
  q_p50 double precision,
  q_max double precision,
  updated_at timestamp with time zone
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT
    r.id,
    r.genus,
    r.species,
    r.source,
    r.mount_medium,
    r.stain,
    r.length_min,
    r.length_p05,
    r.length_p50,
    r.length_p95,
    r.length_max,
    r.width_min,
    r.width_p05,
    r.width_p50,
    r.width_p95,
    r.width_max,
    r.q_min,
    r.q_p50,
    r.q_max,
    r.updated_at
  FROM public.reference_values r
  WHERE lower(r.genus) = lower(trim(coalesce(p_genus, '')))
    AND (trim(coalesce(p_species, '')) = '' OR lower(r.species) = lower(trim(p_species)))
  ORDER BY r.updated_at DESC, r.id DESC
  LIMIT greatest(coalesce(p_limit, 50), 1)
$$;

-- ── 7. Grants ────────────────────────────────────────────────────────────────

REVOKE ALL ON FUNCTION public.are_friends(uuid, uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.can_access_spore_data(uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.community_contributor_label(uuid, text) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION public.search_community_spore_datasets(text, text, int) TO authenticated;
GRANT EXECUTE ON FUNCTION public.get_community_spore_dataset(bigint) TO authenticated;
GRANT EXECUTE ON FUNCTION public.community_spore_taxon_summary(text, text) TO authenticated;
GRANT EXECUTE ON FUNCTION public.search_public_reference_values(text, text, int) TO authenticated;

-- ── 8. Notes for next iteration ──────────────────────────────────────────────
--
-- Helpful future schema improvements:
-- - add user_id and provenance fields to public.reference_values
-- - add calibration_id to public.observation_images for direct QC joins
-- - add explicit contributor visibility controls beyond display_name/username
-- - add policy/view tests once the current production RLS policies are known
