-- Bootstrap schema for future subscription-aware cloud storage.
-- Safe to run before billing is live: defaults preserve today's free behavior.

ALTER TABLE public.profiles
  ADD COLUMN IF NOT EXISTS cloud_plan text NOT NULL DEFAULT 'free',
  ADD COLUMN IF NOT EXISTS full_res_storage_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS storage_quota_bytes bigint,
  ADD COLUMN IF NOT EXISTS storage_used_bytes bigint NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS billing_status text,
  ADD COLUMN IF NOT EXISTS billing_provider text;

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

ALTER TABLE public.observation_images
  ADD COLUMN IF NOT EXISTS upload_mode text,
  ADD COLUMN IF NOT EXISTS source_width integer,
  ADD COLUMN IF NOT EXISTS source_height integer,
  ADD COLUMN IF NOT EXISTS stored_width integer,
  ADD COLUMN IF NOT EXISTS stored_height integer,
  ADD COLUMN IF NOT EXISTS stored_bytes bigint;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'observation_images_upload_mode_check'
  ) THEN
    ALTER TABLE public.observation_images
      ADD CONSTRAINT observation_images_upload_mode_check
      CHECK (upload_mode IS NULL OR upload_mode IN ('reduced', 'full'));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_profiles_cloud_plan
  ON public.profiles (cloud_plan);
