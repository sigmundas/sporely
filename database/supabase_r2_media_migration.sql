-- Cloudflare R2 media migration helpers for Sporely
--
-- Purpose:
-- - keep cloud media references as relative keys instead of full URLs
-- - expose cover image + thumbnail keys on observations
-- - expose image + thumbnail keys on spore_measurements for analysis/QC UIs
--
-- Safe to run more than once.

-- 1. Add media-key columns used by desktop/web analysis views
ALTER TABLE public.observations
  ADD COLUMN IF NOT EXISTS image_key text,
  ADD COLUMN IF NOT EXISTS thumb_key text;

ALTER TABLE public.spore_measurements
  ADD COLUMN IF NOT EXISTS image_key text,
  ADD COLUMN IF NOT EXISTS thumb_key text;

CREATE INDEX IF NOT EXISTS idx_observations_image_key
  ON public.observations (image_key);

CREATE INDEX IF NOT EXISTS idx_spore_measurements_thumb_key
  ON public.spore_measurements (thumb_key);

-- 2. Normalize existing observation_images.storage_path values to relative keys
--    Handles both custom-domain URLs and legacy Supabase Storage URLs.
UPDATE public.observation_images
SET storage_path = regexp_replace(storage_path, '^https?://media\\.sporely\\.no/', '')
WHERE storage_path ~ '^https?://media\.sporely\.no/';

UPDATE public.observation_images
SET storage_path = regexp_replace(
      storage_path,
      '^https?://[^/]+/storage/v1/object(?:/authenticated|/public)?/observation-images/',
      ''
    )
WHERE storage_path ~ '^https?://[^/]+/storage/v1/object(?:/authenticated|/public)?/observation-images/';

UPDATE public.observation_images
SET storage_path = regexp_replace(storage_path, '^https?://[^/]+/sporely-media/', '')
WHERE storage_path ~ '^https?://[^/]+/sporely-media/';

-- 3. Backfill observation cover keys from the first cloud image
WITH ranked_images AS (
  SELECT
    i.observation_id,
    i.storage_path,
    row_number() OVER (
      PARTITION BY i.observation_id
      ORDER BY coalesce(i.sort_order, 2147483647), i.id
    ) AS rn
  FROM public.observation_images i
  WHERE coalesce(i.storage_path, '') <> ''
)
UPDATE public.observations o
SET image_key = r.storage_path,
    thumb_key = regexp_replace(r.storage_path, '(^|/)([^/]+)$', E'\\1thumb_\\2')
FROM ranked_images r
WHERE r.rn = 1
  AND o.id = r.observation_id;

-- 4. Backfill per-measurement image + thumbnail keys from the parent image row
UPDATE public.spore_measurements m
SET image_key = i.storage_path,
    thumb_key = regexp_replace(i.storage_path, '(^|/)([^/]+)$', E'\\1thumb_\\2')
FROM public.observation_images i
WHERE i.id = m.image_id
  AND coalesce(i.storage_path, '') <> '';
