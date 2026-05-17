-- Adds comment mention support for home feed previews and comment composer fallbacks.
-- Safe to apply even if the column already exists.

ALTER TABLE public.comments
  ADD COLUMN IF NOT EXISTS mentioned_user_ids uuid[] DEFAULT '{}'::uuid[];

UPDATE public.comments
SET mentioned_user_ids = '{}'::uuid[]
WHERE mentioned_user_ids IS NULL;

ALTER TABLE public.comments
  ALTER COLUMN mentioned_user_ids SET DEFAULT '{}'::uuid[];

CREATE INDEX IF NOT EXISTS comments_mentioned_user_ids_gin_idx
  ON public.comments
  USING GIN (mentioned_user_ids);
