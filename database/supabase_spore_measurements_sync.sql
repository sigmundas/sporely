-- Spore measurements sync: desktop ↔ Supabase
--
-- Purpose:
-- - Add desktop_id and user_id to public.spore_measurements so the desktop
--   app can upsert by (desktop_id, user_id) without relying on cloud UUIDs.
-- - Add RLS policies so authenticated users can insert/update/delete their
--   own measurement rows.
-- - Add index for efficient upsert lookups.
--
-- Notes:
-- - Community RPCs (SECURITY DEFINER) bypass RLS and can read all rows.
--   These policies govern direct PostgREST access only.
--
-- Apply in Supabase SQL editor (run once; all statements are idempotent).

-- 1. Add sync columns
ALTER TABLE public.spore_measurements
  ADD COLUMN IF NOT EXISTS desktop_id bigint,
  ADD COLUMN IF NOT EXISTS user_id uuid REFERENCES auth.users(id);

-- 2. Unique index for upsert by (desktop_id, user_id)
CREATE UNIQUE INDEX IF NOT EXISTS idx_spore_measurements_desktop_user
  ON public.spore_measurements (desktop_id, user_id)
  WHERE desktop_id IS NOT NULL AND user_id IS NOT NULL;

-- 3. Index for lookup by user
CREATE INDEX IF NOT EXISTS idx_spore_measurements_user_id
  ON public.spore_measurements (user_id);

-- 4. Enable RLS
ALTER TABLE public.spore_measurements ENABLE ROW LEVEL SECURITY;

-- 5. RLS policies (drop + recreate = idempotent)

DROP POLICY IF EXISTS "Users can view their own measurements" ON public.spore_measurements;
CREATE POLICY "Users can view their own measurements"
  ON public.spore_measurements
  FOR SELECT
  USING (user_id = auth.uid());

DROP POLICY IF EXISTS "Users can insert their own measurements" ON public.spore_measurements;
CREATE POLICY "Users can insert their own measurements"
  ON public.spore_measurements
  FOR INSERT
  WITH CHECK (user_id = auth.uid());

DROP POLICY IF EXISTS "Users can update their own measurements" ON public.spore_measurements;
CREATE POLICY "Users can update their own measurements"
  ON public.spore_measurements
  FOR UPDATE
  USING (user_id = auth.uid())
  WITH CHECK (user_id = auth.uid());

DROP POLICY IF EXISTS "Users can delete their own measurements" ON public.spore_measurements;
CREATE POLICY "Users can delete their own measurements"
  ON public.spore_measurements
  FOR DELETE
  USING (user_id = auth.uid());
