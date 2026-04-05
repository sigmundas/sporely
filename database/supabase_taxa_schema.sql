-- Run this in Supabase SQL editor before running import_taxa_to_supabase.py

-- ── Tables ────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.taxa (
  taxon_id             int  PRIMARY KEY,
  genus                text NOT NULL,
  specific_epithet     text NOT NULL,
  canonical_scientific_name text,
  family               text,
  taxon_rank           text,
  norwegian_taxon_id   int,
  swedish_taxon_id     int,
  inaturalist_taxon_id int,
  artportalen_taxon_id int
);

CREATE TABLE IF NOT EXISTS public.taxa_vernacular (
  id              serial PRIMARY KEY,
  taxon_id        int  NOT NULL REFERENCES public.taxa(taxon_id) ON DELETE CASCADE,
  language_code   text NOT NULL,
  vernacular_name text NOT NULL,
  is_preferred    boolean NOT NULL DEFAULT false
);

-- ── Indexes ───────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_taxa_scientific
  ON public.taxa (canonical_scientific_name text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_taxa_genus
  ON public.taxa (genus text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_taxa_genus_species
  ON public.taxa (genus, specific_epithet);
CREATE INDEX IF NOT EXISTS idx_vernacular_name
  ON public.taxa_vernacular (vernacular_name text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_vernacular_lang
  ON public.taxa_vernacular (language_code, is_preferred);
CREATE INDEX IF NOT EXISTS idx_vernacular_taxon
  ON public.taxa_vernacular (taxon_id);

-- ── RLS ───────────────────────────────────────────────────────────────────────

ALTER TABLE public.taxa ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.taxa_vernacular ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "taxa read" ON public.taxa;
DROP POLICY IF EXISTS "vernacular read" ON public.taxa_vernacular;

CREATE POLICY "taxa read"       ON public.taxa            FOR SELECT TO authenticated USING (true);
CREATE POLICY "vernacular read" ON public.taxa_vernacular FOR SELECT TO authenticated USING (true);

-- ── Search RPC ────────────────────────────────────────────────────────────────
-- Returns up to `lim` taxa matching q (prefix) against vernacular OR scientific
-- name. Preferred vernacular name in `lang` is always returned for display.

CREATE OR REPLACE FUNCTION public.search_taxa(
  q    text,
  lang text DEFAULT 'no',
  lim  int  DEFAULT 20
)
RETURNS TABLE (
  taxon_id             int,
  genus                text,
  specific_epithet     text,
  canonical_scientific_name text,
  family               text,
  vernacular_name      text,
  norwegian_taxon_id   int,
  swedish_taxon_id     int,
  inaturalist_taxon_id int,
  artportalen_taxon_id int,
  match_type           text
)
LANGUAGE sql STABLE SECURITY DEFINER
AS $$
  WITH candidates AS (
    SELECT DISTINCT t.taxon_id, t.genus, t.specific_epithet,
      CASE WHEN EXISTS (
        SELECT 1 FROM public.taxa_vernacular mv
        WHERE mv.taxon_id = t.taxon_id
          AND mv.vernacular_name ILIKE q || '%'
      ) THEN 0 ELSE 1 END AS score
    FROM public.taxa t
    WHERE
      EXISTS (
        SELECT 1 FROM public.taxa_vernacular mv
        WHERE mv.taxon_id = t.taxon_id
          AND mv.vernacular_name ILIKE q || '%'
      )
      OR t.canonical_scientific_name ILIKE q || '%'
      OR (t.genus || ' ' || t.specific_epithet) ILIKE q || '%'
      OR t.genus ILIKE q || '%'
    ORDER BY score, t.genus, t.specific_epithet
    LIMIT lim
  )
  SELECT
    t.taxon_id,
    t.genus,
    t.specific_epithet,
    t.canonical_scientific_name,
    t.family,
    pv.vernacular_name,
    t.norwegian_taxon_id,
    t.swedish_taxon_id,
    t.inaturalist_taxon_id,
    t.artportalen_taxon_id,
    CASE WHEN c.score = 0 THEN 'vernacular' ELSE 'scientific' END AS match_type
  FROM candidates c
  JOIN public.taxa t ON t.taxon_id = c.taxon_id
  LEFT JOIN public.taxa_vernacular pv
    ON pv.taxon_id = t.taxon_id
   AND pv.language_code = lang
   AND pv.is_preferred = true
  ORDER BY c.score, t.genus, t.specific_epithet
$$;
