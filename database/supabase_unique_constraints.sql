-- Run once in the Supabase SQL editor.
-- Adds UNIQUE constraints on desktop_id so the sync client can do efficient
-- check-and-update without needing to fetch first.

ALTER TABLE public.observations
    ADD CONSTRAINT observations_desktop_id_user_unique
    UNIQUE (desktop_id, user_id);

ALTER TABLE public.observation_images
    ADD CONSTRAINT observation_images_desktop_id_user_unique
    UNIQUE (desktop_id, user_id);
