-- Phase 7 change-of-plans patch: Transparency, social trails, and privacy slots.
--
-- Apply after supabase_phase7_privacy_social_costs.sql.
-- This moves workflow state out of observation visibility:
--   is_draft = true/false controls WIP vs finished
--   visibility = private/friends/public controls who can see the observation
--   location_precision = exact/fuzzed controls whether public feeds show exact GPS

-- ── 1. Public drafts by default ─────────────────────────────────────────────

ALTER TABLE public.profiles
  ADD COLUMN IF NOT EXISTS is_pro boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS cloud_plan text NOT NULL DEFAULT 'free';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'profiles_cloud_plan_check'
  ) THEN
    ALTER TABLE public.profiles
      ADD CONSTRAINT profiles_cloud_plan_check
      CHECK (cloud_plan IN ('free', 'pro'));
  END IF;
END $$;

UPDATE public.profiles
SET is_pro = true
WHERE is_pro = false
  AND cloud_plan = 'pro';

CREATE OR REPLACE FUNCTION public.profile_has_pro_access(profile_id uuid)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT coalesce(p.is_pro, false) OR coalesce(p.cloud_plan, 'free') = 'pro'
  FROM public.profiles p
  WHERE p.id = profile_id
$$;

ALTER TABLE public.observations
  ADD COLUMN IF NOT EXISTS is_draft boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS location_precision text NOT NULL DEFAULT 'exact',
  ADD COLUMN IF NOT EXISTS visibility text NOT NULL DEFAULT 'public';

ALTER TABLE public.observations
  DROP CONSTRAINT IF EXISTS observations_visibility_check,
  DROP CONSTRAINT IF EXISTS observations_location_precision_check;

UPDATE public.observations
SET visibility = CASE
  WHEN visibility IN ('draft', 'private') THEN 'private'
  WHEN visibility IN ('friends', 'public') THEN visibility
  ELSE 'public'
END
WHERE visibility IS NULL
   OR visibility NOT IN ('private', 'friends', 'public')
   OR visibility = 'draft';

UPDATE public.observations
SET location_precision = 'exact'
WHERE location_precision IS NULL
   OR location_precision NOT IN ('exact', 'fuzzed');

UPDATE public.observations
SET is_draft = true
WHERE is_draft IS NULL;

ALTER TABLE public.observations
  ALTER COLUMN visibility SET DEFAULT 'public',
  ALTER COLUMN visibility SET NOT NULL,
  ALTER COLUMN is_draft SET DEFAULT true,
  ALTER COLUMN is_draft SET NOT NULL,
  ALTER COLUMN location_precision SET DEFAULT 'exact',
  ALTER COLUMN location_precision SET NOT NULL;

ALTER TABLE public.observations
  ADD CONSTRAINT observations_visibility_check
  CHECK (visibility IN ('private', 'friends', 'public'));

ALTER TABLE public.observations
  ADD CONSTRAINT observations_location_precision_check
  CHECK (location_precision IN ('exact', 'fuzzed'));

CREATE INDEX IF NOT EXISTS idx_observations_is_draft
  ON public.observations (is_draft);

CREATE INDEX IF NOT EXISTS idx_observations_location_precision
  ON public.observations (location_precision);

CREATE INDEX IF NOT EXISTS idx_observations_user_visibility
  ON public.observations (user_id, visibility);

-- ── 2. Privacy slot accounting ─────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.can_read_observation(
  owner_id uuid,
  visibility_value text
)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT CASE
    WHEN auth.uid() = owner_id THEN true
    WHEN EXISTS (
      SELECT 1 FROM public.profiles p
      WHERE p.id = owner_id
        AND p.is_banned = true
    ) THEN false
    WHEN public.is_blocked_between(auth.uid(), owner_id) THEN false
    WHEN coalesce(visibility_value, 'public') = 'public' THEN true
    WHEN coalesce(visibility_value, 'public') = 'friends'
      THEN public.are_friends(auth.uid(), owner_id)
    ELSE false
  END
$$;

CREATE OR REPLACE FUNCTION public.non_public_observation_count(profile_id uuid)
RETURNS integer
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT count(*)::integer
  FROM public.observations o
  WHERE o.user_id = profile_id
    AND (
      coalesce(o.visibility, 'public') <> 'public'
      OR coalesce(o.location_precision, 'exact') = 'fuzzed'
    )
$$;

CREATE OR REPLACE FUNCTION public.enforce_non_public_observation_limit()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  current_count integer;
BEGIN
  IF coalesce(NEW.visibility, 'public') = 'public'
     AND coalesce(NEW.location_precision, 'exact') = 'exact' THEN
    RETURN NEW;
  END IF;

  IF public.profile_has_pro_access(NEW.user_id) THEN
    RETURN NEW;
  END IF;

  SELECT count(*)::integer
  INTO current_count
  FROM public.observations o
  WHERE o.user_id = NEW.user_id
    AND (
      coalesce(o.visibility, 'public') <> 'public'
      OR coalesce(o.location_precision, 'exact') = 'fuzzed'
    )
    AND (TG_OP = 'INSERT' OR o.id <> NEW.id);

  IF current_count >= 20 THEN
    RAISE EXCEPTION
      'Free Sporely accounts can keep up to 20 privacy slot observations. Publish or use exact public location to continue.'
      USING ERRCODE = 'check_violation';
  END IF;

  RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS enforce_non_public_observation_limit_trigger
  ON public.observations;

CREATE TRIGGER enforce_non_public_observation_limit_trigger
BEFORE INSERT OR UPDATE OF user_id, visibility, location_precision
ON public.observations
FOR EACH ROW
EXECUTE FUNCTION public.enforce_non_public_observation_limit();

-- ── 3. Follows table ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.follows (
  user_id uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  target_type text NOT NULL,
  target_id text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, target_type, target_id),
  CONSTRAINT follows_target_type_check
    CHECK (target_type IN ('user', 'observation', 'species', 'genus')),
  CONSTRAINT follows_target_id_not_blank
    CHECK (length(trim(target_id)) > 0)
);

CREATE INDEX IF NOT EXISTS idx_follows_target
  ON public.follows (target_type, target_id);

ALTER TABLE public.follows ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "phase7_follows_read_own" ON public.follows;
DROP POLICY IF EXISTS "phase7_follows_insert_own" ON public.follows;
DROP POLICY IF EXISTS "phase7_follows_delete_own" ON public.follows;

CREATE POLICY "phase7_follows_read_own"
  ON public.follows
  FOR SELECT
  TO authenticated
  USING (auth.uid() = user_id);

CREATE POLICY "phase7_follows_insert_own"
  ON public.follows
  FOR INSERT
  TO authenticated
  WITH CHECK (auth.uid() = user_id);

CREATE POLICY "phase7_follows_delete_own"
  ON public.follows
  FOR DELETE
  TO authenticated
  USING (auth.uid() = user_id);

-- ── 4. Feed views ──────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW public.observations_friend_view AS
SELECT o.id,
       o.user_id,
       o.desktop_id,
       o.date,
       o.captured_at,
       o.created_at,
       o.genus,
       o.species,
       o.common_name,
       o.author,
       o.location,
       o.habitat,
       o.notes,
       o.uncertain,
       o.location_public,
       o.visibility,
       CASE
         WHEN coalesce(o.location_precision, 'exact') = 'fuzzed'
           THEN round(o.gps_latitude::numeric, 2)::double precision
         ELSE o.gps_latitude
       END AS gps_latitude,
       CASE
         WHEN coalesce(o.location_precision, 'exact') = 'fuzzed'
           THEN round(o.gps_longitude::numeric, 2)::double precision
         ELSE o.gps_longitude
       END AS gps_longitude,
       o.source_type,
       o.spore_data_visibility,
       o.image_key,
       o.thumb_key,
       o.is_draft,
       o.location_precision
FROM public.observations o
WHERE coalesce(o.visibility, 'public') IN ('friends', 'public')
  AND public.are_friends(auth.uid(), o.user_id)
  AND NOT EXISTS (
    SELECT 1 FROM public.profiles p
    WHERE p.id = o.user_id
      AND p.is_banned = true
  )
  AND NOT public.is_blocked_between(auth.uid(), o.user_id);

CREATE OR REPLACE VIEW public.observations_community_view AS
SELECT o.id,
       o.user_id,
       o.desktop_id,
       o.date,
       o.captured_at,
       o.created_at,
       o.genus,
       o.species,
       o.common_name,
       o.author,
       o.location,
       o.habitat,
       o.notes,
       o.uncertain,
       o.location_public,
       o.visibility,
       CASE
         WHEN coalesce(o.location_precision, 'exact') = 'fuzzed'
           THEN round(o.gps_latitude::numeric, 2)::double precision
         ELSE o.gps_latitude
       END AS gps_latitude,
       CASE
         WHEN coalesce(o.location_precision, 'exact') = 'fuzzed'
           THEN round(o.gps_longitude::numeric, 2)::double precision
         ELSE o.gps_longitude
       END AS gps_longitude,
       o.source_type,
       o.spore_data_visibility,
       o.image_key,
       o.thumb_key,
       o.is_draft,
       o.location_precision
FROM public.observations o
WHERE coalesce(o.visibility, 'public') = 'public'
  AND NOT EXISTS (
    SELECT 1 FROM public.profiles p
    WHERE p.id = o.user_id
      AND p.is_banned = true
  )
  AND NOT public.is_blocked_between(auth.uid(), o.user_id);

DROP VIEW IF EXISTS public.observations_follow_view;

CREATE VIEW public.observations_follow_view AS
SELECT DISTINCT o.id,
       o.user_id,
       o.desktop_id,
       o.date,
       o.captured_at,
       o.created_at,
       o.genus,
       o.species,
       o.common_name,
       o.author,
       o.location,
       o.habitat,
       o.notes,
       o.uncertain,
       o.location_public,
       o.visibility,
       CASE
         WHEN coalesce(o.location_precision, 'exact') = 'fuzzed'
           THEN round(o.gps_latitude::numeric, 2)::double precision
         ELSE o.gps_latitude
       END AS gps_latitude,
       CASE
         WHEN coalesce(o.location_precision, 'exact') = 'fuzzed'
           THEN round(o.gps_longitude::numeric, 2)::double precision
         ELSE o.gps_longitude
       END AS gps_longitude,
       o.source_type,
       o.spore_data_visibility,
       o.image_key,
       o.thumb_key,
       o.is_draft,
       o.location_precision
FROM public.observations o
JOIN public.follows f
  ON f.user_id = auth.uid()
 AND (
      (f.target_type = 'user' AND f.target_id = o.user_id::text)
   OR (f.target_type = 'observation' AND f.target_id = o.id::text)
   OR (f.target_type = 'genus' AND lower(f.target_id) = lower(coalesce(o.genus, '')))
   OR (
        f.target_type = 'species'
        AND lower(f.target_id) = lower(trim(concat_ws(' ', o.genus, o.species)))
      )
 )
WHERE public.can_read_observation(o.user_id, o.visibility)
  AND NOT EXISTS (
    SELECT 1 FROM public.profiles p
    WHERE p.id = o.user_id
      AND p.is_banned = true
  )
  AND NOT public.is_blocked_between(auth.uid(), o.user_id);

GRANT SELECT ON public.observations_friend_view TO authenticated;
GRANT SELECT ON public.observations_community_view TO authenticated;
GRANT SELECT ON public.observations_follow_view TO authenticated;
GRANT SELECT, INSERT, DELETE ON public.follows TO authenticated;

REVOKE ALL ON FUNCTION public.profile_has_pro_access(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.non_public_observation_count(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.non_public_observation_count(uuid) TO authenticated;
