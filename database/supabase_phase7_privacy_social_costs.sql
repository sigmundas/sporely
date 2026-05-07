-- Phase 7: Privacy, social feeds, and non-public storage limits.
--
-- Apply after the existing profiles/friendships/blocks schema is present.
-- This migration moves cloud observation visibility to the canonical
-- draft/friends/public model while keeping older cloud_plan state compatible
-- with the new profiles.is_pro flag.

-- ── 1. Canonical pro flag ───────────────────────────────────────────────────

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

CREATE INDEX IF NOT EXISTS idx_profiles_is_pro
  ON public.profiles (is_pro);

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

-- ── 2. Observation visibility model ─────────────────────────────────────────

ALTER TABLE public.observations
  ADD COLUMN IF NOT EXISTS visibility text NOT NULL DEFAULT 'draft';

UPDATE public.observations
SET visibility = CASE
  WHEN visibility = 'private' THEN 'draft'
  WHEN visibility IN ('draft', 'friends', 'public') THEN visibility
  ELSE 'draft'
END
WHERE visibility IS NULL
   OR visibility NOT IN ('draft', 'friends', 'public')
   OR visibility = 'private';

ALTER TABLE public.observations
  ALTER COLUMN visibility SET DEFAULT 'draft';

ALTER TABLE public.observations
  DROP CONSTRAINT IF EXISTS observations_visibility_check;

ALTER TABLE public.observations
  ADD CONSTRAINT observations_visibility_check
  CHECK (visibility IN ('draft', 'friends', 'public'));

CREATE INDEX IF NOT EXISTS idx_observations_visibility
  ON public.observations (visibility);

CREATE INDEX IF NOT EXISTS idx_observations_user_visibility
  ON public.observations (user_id, visibility);

-- ── 3. Friendship and block helpers ─────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.are_friends(user_a uuid, user_b uuid)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT user_a IS NOT NULL
     AND user_b IS NOT NULL
     AND user_a <> user_b
     AND EXISTS (
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

CREATE OR REPLACE FUNCTION public.is_blocked_between(user_a uuid, user_b uuid)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT user_a IS NOT NULL
     AND user_b IS NOT NULL
     AND EXISTS (
       SELECT 1
       FROM public.user_blocks ub
       WHERE (ub.blocker_id = user_a AND ub.blocked_id = user_b)
          OR (ub.blocker_id = user_b AND ub.blocked_id = user_a)
     )
$$;

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
    WHEN coalesce(visibility_value, 'draft') = 'public' THEN true
    WHEN coalesce(visibility_value, 'draft') = 'friends'
      THEN public.are_friends(auth.uid(), owner_id)
    ELSE false
  END
$$;

CREATE OR REPLACE FUNCTION public.can_see_exact_observation_location(
  observation_id bigint,
  owner_id uuid
)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT auth.uid() = owner_id
      OR public.are_friends(auth.uid(), owner_id)
      OR EXISTS (
        SELECT 1
        FROM public.observation_shares s
        WHERE s.shared_with_id = auth.uid()
          AND s.observation_id = observation_id
      )
$$;

-- ── 4. Free-tier non-public limit ───────────────────────────────────────────

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
    AND coalesce(o.visibility, 'draft') IN ('draft', 'friends')
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
  IF coalesce(NEW.visibility, 'draft') NOT IN ('draft', 'friends') THEN
    RETURN NEW;
  END IF;

  IF public.profile_has_pro_access(NEW.user_id) THEN
    RETURN NEW;
  END IF;

  SELECT count(*)::integer
  INTO current_count
  FROM public.observations o
  WHERE o.user_id = NEW.user_id
    AND coalesce(o.visibility, 'draft') IN ('draft', 'friends')
    AND (TG_OP = 'INSERT' OR o.id <> NEW.id);

  IF current_count >= 20 THEN
    RAISE EXCEPTION
      'Free Sporely accounts can keep up to 20 draft or friends-only observations. Publish or delete one to continue.'
      USING ERRCODE = 'check_violation';
  END IF;

  RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS enforce_non_public_observation_limit_trigger
  ON public.observations;

CREATE TRIGGER enforce_non_public_observation_limit_trigger
BEFORE INSERT OR UPDATE OF user_id, visibility
ON public.observations
FOR EACH ROW
EXECUTE FUNCTION public.enforce_non_public_observation_limit();

-- ── 5. RLS policies ─────────────────────────────────────────────────────────

ALTER TABLE public.observations ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.observation_images ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "phase7_observations_read" ON public.observations;
DROP POLICY IF EXISTS "phase7_observations_insert_own" ON public.observations;
DROP POLICY IF EXISTS "phase7_observations_update_own" ON public.observations;
DROP POLICY IF EXISTS "phase7_observations_delete_own" ON public.observations;

CREATE POLICY "phase7_observations_read"
  ON public.observations
  FOR SELECT
  TO authenticated
  USING (
    -- Stranger/public access uses observations_community_view so exact GPS is
    -- never exposed from the base table.
    auth.uid() = user_id
    OR (
      coalesce(visibility, 'draft') IN ('friends', 'public')
      AND public.are_friends(auth.uid(), user_id)
      AND NOT public.is_blocked_between(auth.uid(), user_id)
    )
  );

CREATE POLICY "phase7_observations_insert_own"
  ON public.observations
  FOR INSERT
  TO authenticated
  WITH CHECK (auth.uid() = user_id);

CREATE POLICY "phase7_observations_update_own"
  ON public.observations
  FOR UPDATE
  TO authenticated
  USING (auth.uid() = user_id)
  WITH CHECK (auth.uid() = user_id);

CREATE POLICY "phase7_observations_delete_own"
  ON public.observations
  FOR DELETE
  TO authenticated
  USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "phase7_observation_images_read" ON public.observation_images;
DROP POLICY IF EXISTS "phase7_observation_images_insert_own" ON public.observation_images;
DROP POLICY IF EXISTS "phase7_observation_images_update_own" ON public.observation_images;
DROP POLICY IF EXISTS "phase7_observation_images_delete_own" ON public.observation_images;

CREATE POLICY "phase7_observation_images_read"
  ON public.observation_images
  FOR SELECT
  TO authenticated
  USING (
    EXISTS (
      SELECT 1
      FROM public.observations o
      WHERE o.id = observation_images.observation_id
        AND public.can_read_observation(o.user_id, o.visibility)
    )
  );

CREATE POLICY "phase7_observation_images_insert_own"
  ON public.observation_images
  FOR INSERT
  TO authenticated
  WITH CHECK (
    auth.uid() = user_id
    AND storage_path LIKE auth.uid()::text || '/%'
    AND EXISTS (
      SELECT 1
      FROM public.observations o
      WHERE o.id = observation_images.observation_id
        AND o.user_id = auth.uid()
    )
  );

CREATE POLICY "phase7_observation_images_update_own"
  ON public.observation_images
  FOR UPDATE
  TO authenticated
  USING (auth.uid() = user_id)
  WITH CHECK (
    auth.uid() = user_id
    AND (storage_path IS NULL OR storage_path LIKE auth.uid()::text || '/%')
  );

CREATE POLICY "phase7_observation_images_delete_own"
  ON public.observation_images
  FOR DELETE
  TO authenticated
  USING (auth.uid() = user_id);

DO $$
BEGIN
  IF to_regclass('public.comments') IS NOT NULL
     AND EXISTS (
       SELECT 1 FROM information_schema.columns
       WHERE table_schema = 'public'
         AND table_name = 'comments'
         AND column_name = 'observation_id'
     )
     AND EXISTS (
       SELECT 1 FROM information_schema.columns
       WHERE table_schema = 'public'
         AND table_name = 'comments'
         AND column_name = 'user_id'
     ) THEN
    EXECUTE 'ALTER TABLE public.comments ENABLE ROW LEVEL SECURITY';
    EXECUTE 'DROP POLICY IF EXISTS "phase7_comments_read" ON public.comments';
    EXECUTE 'DROP POLICY IF EXISTS "phase7_comments_insert_visible" ON public.comments';
    EXECUTE 'DROP POLICY IF EXISTS "phase7_comments_update_own" ON public.comments';
    EXECUTE 'DROP POLICY IF EXISTS "phase7_comments_delete_own" ON public.comments';

    EXECUTE $policy$
      CREATE POLICY "phase7_comments_read"
        ON public.comments
        FOR SELECT
        TO authenticated
        USING (
          EXISTS (
            SELECT 1
            FROM public.observations o
            WHERE o.id = comments.observation_id
              AND public.can_read_observation(o.user_id, o.visibility)
          )
        )
    $policy$;

    EXECUTE $policy$
      CREATE POLICY "phase7_comments_insert_visible"
        ON public.comments
        FOR INSERT
        TO authenticated
        WITH CHECK (
          auth.uid() = user_id
          AND EXISTS (
            SELECT 1
            FROM public.observations o
            WHERE o.id = comments.observation_id
              AND public.can_read_observation(o.user_id, o.visibility)
          )
        )
    $policy$;

    EXECUTE $policy$
      CREATE POLICY "phase7_comments_update_own"
        ON public.comments
        FOR UPDATE
        TO authenticated
        USING (auth.uid() = user_id)
        WITH CHECK (auth.uid() = user_id)
    $policy$;

    EXECUTE $policy$
      CREATE POLICY "phase7_comments_delete_own"
        ON public.comments
        FOR DELETE
        TO authenticated
        USING (auth.uid() = user_id)
    $policy$;
  END IF;
END $$;

-- ── 6. Privacy-preserving feed views ────────────────────────────────────────

DROP VIEW IF EXISTS public.observations_community_view;
DROP VIEW IF EXISTS public.observations_friend_view;

CREATE VIEW public.observations_friend_view AS
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
       o.gps_latitude,
       o.gps_longitude,
       o.source_type,
       o.spore_data_visibility,
       o.image_key,
       o.thumb_key
FROM public.observations o
WHERE coalesce(o.visibility, 'draft') IN ('friends', 'public')
  AND public.are_friends(auth.uid(), o.user_id)
  AND NOT EXISTS (
    SELECT 1 FROM public.profiles p
    WHERE p.id = o.user_id
      AND p.is_banned = true
  )
  AND NOT public.is_blocked_between(auth.uid(), o.user_id);

CREATE VIEW public.observations_community_view AS
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
       CASE
         WHEN public.can_see_exact_observation_location(o.id, o.user_id) THEN o.location
         ELSE NULL::text
       END AS location,
       o.habitat,
       CASE
         WHEN public.can_see_exact_observation_location(o.id, o.user_id) THEN o.notes
         ELSE NULL::text
       END AS notes,
       o.uncertain,
       o.location_public,
       o.visibility,
       CASE
         WHEN public.can_see_exact_observation_location(o.id, o.user_id) THEN o.gps_latitude
         ELSE round(o.gps_latitude::numeric, 2)::double precision
       END AS gps_latitude,
       CASE
         WHEN public.can_see_exact_observation_location(o.id, o.user_id) THEN o.gps_longitude
         ELSE round(o.gps_longitude::numeric, 2)::double precision
       END AS gps_longitude,
       o.source_type,
       o.spore_data_visibility,
       o.image_key,
       o.thumb_key
FROM public.observations o
WHERE coalesce(o.visibility, 'draft') = 'public'
  AND NOT EXISTS (
    SELECT 1 FROM public.profiles p
    WHERE p.id = o.user_id
      AND p.is_banned = true
  )
  AND NOT public.is_blocked_between(auth.uid(), o.user_id);

GRANT SELECT ON public.observations_friend_view TO authenticated;
GRANT SELECT ON public.observations_community_view TO authenticated;

REVOKE ALL ON FUNCTION public.profile_has_pro_access(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.non_public_observation_count(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.non_public_observation_count(uuid) TO authenticated;
