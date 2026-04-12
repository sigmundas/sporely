ALTER TABLE public.observation_images
  ADD COLUMN IF NOT EXISTS ai_crop_x1 double precision,
  ADD COLUMN IF NOT EXISTS ai_crop_y1 double precision,
  ADD COLUMN IF NOT EXISTS ai_crop_x2 double precision,
  ADD COLUMN IF NOT EXISTS ai_crop_y2 double precision,
  ADD COLUMN IF NOT EXISTS ai_crop_source_w integer,
  ADD COLUMN IF NOT EXISTS ai_crop_source_h integer;
