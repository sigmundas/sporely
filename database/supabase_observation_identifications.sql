-- Observation AI identification cache/history.
-- Owner-only access: users can manage their own observation identifications.

CREATE TABLE IF NOT EXISTS public.observation_identifications (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  observation_id uuid NOT NULL REFERENCES public.observations(id) ON DELETE CASCADE,
  user_id uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  service text NOT NULL CHECK (service IN ('artsorakel', 'inat')),
  source text NOT NULL DEFAULT 'ai',
  status text NOT NULL DEFAULT 'success' CHECK (status IN ('success', 'no_match', 'error', 'stale')),
  image_fingerprint text NOT NULL,
  crop_fingerprint text,
  request_fingerprint text NOT NULL,
  language text,
  model_version text,
  results jsonb NOT NULL DEFAULT '[]'::jsonb,
  top_scientific_name text,
  top_vernacular_name text,
  top_taxon_id text,
  top_probability numeric,
  error_message text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS observation_identifications_observation_service_request_idx
  ON public.observation_identifications (observation_id, service, request_fingerprint);

CREATE INDEX IF NOT EXISTS observation_identifications_observation_service_idx
  ON public.observation_identifications (observation_id, service);

CREATE INDEX IF NOT EXISTS observation_identifications_observation_request_idx
  ON public.observation_identifications (observation_id, request_fingerprint);

CREATE INDEX IF NOT EXISTS observation_identifications_user_created_idx
  ON public.observation_identifications (user_id, created_at DESC);

ALTER TABLE public.observation_identifications ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS observation_identifications_select_own ON public.observation_identifications;
CREATE POLICY observation_identifications_select_own
  ON public.observation_identifications
  FOR SELECT
  TO authenticated
  USING (
    user_id = auth.uid()
    AND EXISTS (
      SELECT 1
      FROM public.observations o
      WHERE o.id = observation_identifications.observation_id
        AND o.user_id = auth.uid()
    )
  );

DROP POLICY IF EXISTS observation_identifications_insert_own ON public.observation_identifications;
CREATE POLICY observation_identifications_insert_own
  ON public.observation_identifications
  FOR INSERT
  TO authenticated
  WITH CHECK (
    user_id = auth.uid()
    AND EXISTS (
      SELECT 1
      FROM public.observations o
      WHERE o.id = observation_identifications.observation_id
        AND o.user_id = auth.uid()
    )
  );

DROP POLICY IF EXISTS observation_identifications_update_own ON public.observation_identifications;
CREATE POLICY observation_identifications_update_own
  ON public.observation_identifications
  FOR UPDATE
  TO authenticated
  USING (
    user_id = auth.uid()
    AND EXISTS (
      SELECT 1
      FROM public.observations o
      WHERE o.id = observation_identifications.observation_id
        AND o.user_id = auth.uid()
    )
  )
  WITH CHECK (
    user_id = auth.uid()
    AND EXISTS (
      SELECT 1
      FROM public.observations o
      WHERE o.id = observation_identifications.observation_id
        AND o.user_id = auth.uid()
    )
  );

DROP POLICY IF EXISTS observation_identifications_delete_own ON public.observation_identifications;
CREATE POLICY observation_identifications_delete_own
  ON public.observation_identifications
  FOR DELETE
  TO authenticated
  USING (
    user_id = auth.uid()
    AND EXISTS (
      SELECT 1
      FROM public.observations o
      WHERE o.id = observation_identifications.observation_id
        AND o.user_id = auth.uid()
    )
  );

CREATE OR REPLACE FUNCTION public.set_observation_identifications_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS observation_identifications_set_updated_at ON public.observation_identifications;
CREATE TRIGGER observation_identifications_set_updated_at
BEFORE UPDATE ON public.observation_identifications
FOR EACH ROW
EXECUTE FUNCTION public.set_observation_identifications_updated_at();
