-- ==============================================================================
-- Moderation Views Patch (with DROP)
-- Recreates the community and friend views to enforce Block and Ban filters
-- ==============================================================================

-- 1. Drop the old views to avoid column order conflicts
DROP VIEW IF EXISTS public.observations_community_view;
DROP VIEW IF EXISTS public.observations_friend_view;

-- 2. Create Community View (Public feed)
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
WHERE coalesce(o.visibility, 'private') = 'public'
  AND NOT EXISTS (SELECT 1 FROM public.profiles p WHERE p.id = o.user_id AND p.is_banned = true)
  AND NOT EXISTS (SELECT 1 FROM public.user_blocks ub WHERE (ub.blocker_id = auth.uid() AND ub.blocked_id = o.user_id) OR (ub.blocker_id = o.user_id AND ub.blocked_id = auth.uid()));

-- 3. Create Friend View (Friends feed with conditional GPS)
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
       CASE 
           WHEN o.location_public = true THEN o.gps_latitude
           WHEN EXISTS (SELECT 1 FROM public.observation_shares s WHERE s.observation_id = o.id AND s.shared_with_id = auth.uid()) THEN o.gps_latitude
           ELSE NULL::double precision
       END AS gps_latitude,
       CASE 
           WHEN o.location_public = true THEN o.gps_longitude
           WHEN EXISTS (SELECT 1 FROM public.observation_shares s WHERE s.observation_id = o.id AND s.shared_with_id = auth.uid()) THEN o.gps_longitude
           ELSE NULL::double precision
       END AS gps_longitude,
       o.source_type,
       o.spore_data_visibility,
       o.image_key,
       o.thumb_key
FROM public.observations o
WHERE coalesce(o.visibility, 'private') IN ('public', 'friends')
  AND public.are_friends(auth.uid(), o.user_id)
  AND NOT EXISTS (SELECT 1 FROM public.profiles p WHERE p.id = o.user_id AND p.is_banned = true)
  AND NOT EXISTS (SELECT 1 FROM public.user_blocks ub WHERE (ub.blocker_id = auth.uid() AND ub.blocked_id = o.user_id) OR (ub.blocker_id = o.user_id AND ub.blocked_id = auth.uid()));